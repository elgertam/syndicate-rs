use noise_protocol::CipherState;
use noise_protocol::U8Array;
use noise_protocol::patterns::HandshakePattern;
use noise_rust_crypto::Blake2s;
use noise_rust_crypto::ChaCha20Poly1305;
use noise_rust_crypto::X25519;

use std::convert::TryInto;
use std::sync::Arc;

use preserves_schema::Codec;

use syndicate::actor::*;
use syndicate::relay::Mutex;
use syndicate::relay::TunnelRelay;
use syndicate::rpc;
use syndicate::trace::TurnCause;
use syndicate::value::NestedValue;
use syndicate::value::NoEmbeddedDomainCodec;
use syndicate::value::PackedWriter;

use syndicate::enclose;
use syndicate_macros::during;
use syndicate_macros::pattern;

use syndicate::schemas::dataspace;
use syndicate::schemas::gatekeeper;
use syndicate::schemas::noise;
use syndicate::schemas::rpc as R;
use syndicate::schemas::sturdy;

use crate::language;

fn noise_step_type() -> String {
    language().unparse(&noise::NoiseStepType).value().to_symbol().unwrap().clone()
}

pub fn handle_noise_binds(t: &mut Activation, ds: &Arc<Cap>) -> ActorResult {
    during!(t, ds, language(), <bind <noise $desc> $target $observer>, |t: &mut Activation| {
        t.spawn_link(None, move |t| {
            target.value().to_embedded()?;
            let observer = language().parse::<gatekeeper::BindObserver>(&observer)?;
            let spec = language().parse::<noise::NoiseDescriptionDetail<AnyValue>>(&desc)?.0;
            match validate_noise_service_spec(spec) {
                Ok(spec) => if let gatekeeper::BindObserver::Present(o) = observer {
                    o.assert(t, language(), &gatekeeper::Bound::Bound {
                        path_step: Box::new(gatekeeper::PathStep {
                            step_type: noise_step_type(),
                            detail: language().unparse(&noise::NoisePathStepDetail(noise::NoiseSpec {
                                key: spec.public_key,
                                service: noise::ServiceSelector(spec.service),
                                protocol: if spec.protocol == default_noise_protocol() {
                                    noise::NoiseProtocol::Absent
                                } else {
                                    noise::NoiseProtocol::Present {
                                        protocol: spec.protocol,
                                    }
                                },
                                pre_shared_keys: if spec.psks.is_empty() {
                                    noise::NoisePreSharedKeys::Absent
                                } else {
                                    noise::NoisePreSharedKeys::Present {
                                        pre_shared_keys: spec.psks,
                                    }
                                },
                            })),
                        }),
                    });
                },
                Err(e) => {
                    if let gatekeeper::BindObserver::Present(o) = observer {
                        o.assert(t, language(), &gatekeeper::Bound::Rejected(
                            Box::new(gatekeeper::Rejected {
                                detail: AnyValue::new(format!("{}", &e)),
                            })));
                    }
                    tracing::error!("Invalid noise bind description: {}", e);
                }
            }
            Ok(())
        });
        Ok(())
    });
    Ok(())
}

pub fn take_noise_step(t: &mut Activation, ds: &mut Arc<Cap>, a: &gatekeeper::Resolve, detail: &mut &'static str) -> Result<bool, ActorError> {
    if a.step.step_type == noise_step_type() {
        *detail = "invalid";
        if let Ok(s) = language().parse::<noise::NoiseStepDetail<AnyValue>>(&a.step.detail) {
            t.facet(|t| {
                let f = super::handle_direct_resolution(ds, t, a.clone())?;
                await_bind_noise(ds, t, s.0.0, a.observer.clone(), f)
            })?;
            return Ok(true);
        }
    }
    Ok(false)
}

struct ValidatedNoiseSpec {
    service: AnyValue,
    protocol: String,
    pattern: HandshakePattern,
    psks: Vec<Vec<u8>>,
    secret_key: Option<Vec<u8>>,
    public_key: Vec<u8>,
}

fn default_noise_protocol() -> String {
    language().unparse(&noise::DefaultProtocol).value().to_string().unwrap().clone()
}

fn validate_noise_spec(
    spec: noise::NoiseSpec<AnyValue>,
) -> Result<ValidatedNoiseSpec, ActorError> {
    let protocol = match spec.protocol {
        noise::NoiseProtocol::Present { protocol } => protocol,
        noise::NoiseProtocol::Invalid { protocol } =>
            Err(format!("Invalid noise protocol {:?}", protocol))?,
        noise::NoiseProtocol::Absent => default_noise_protocol(),
    };

    const PREFIX: &'static str = "Noise_";
    const SUFFIX: &'static str = "_25519_ChaChaPoly_BLAKE2s";
    if !protocol.starts_with(PREFIX) || !protocol.ends_with(SUFFIX) {
        Err(format!("Unsupported protocol {:?}", protocol))?;
    }

    let pattern_name = &protocol[PREFIX.len()..(protocol.len()-SUFFIX.len())];
    let pattern = lookup_pattern(pattern_name).ok_or_else::<ActorError, _>(
        || format!("Unsupported handshake pattern {:?}", pattern_name).into())?;

    let psks = match spec.pre_shared_keys {
        noise::NoisePreSharedKeys::Present { pre_shared_keys } => pre_shared_keys,
        noise::NoisePreSharedKeys::Invalid { pre_shared_keys } =>
            Err(format!("Invalid pre-shared-keys {:?}", pre_shared_keys))?,
        noise::NoisePreSharedKeys::Absent => vec![],
    };

    Ok(ValidatedNoiseSpec {
        service: spec.service.0,
        protocol,
        pattern,
        psks,
        secret_key: None,
        public_key: spec.key,
    })
}

fn validate_noise_service_spec(
    spec: noise::NoiseServiceSpec<AnyValue>,
) -> Result<ValidatedNoiseSpec, ActorError> {
    let noise::NoiseServiceSpec { base, secret_key } = spec;
    let v = validate_noise_spec(base)?;
    let secret_key = match secret_key {
        noise::SecretKeyField::Present { secret_key } => Some(secret_key),
        noise::SecretKeyField::Invalid { secret_key } =>
            Err(format!("Invalid secret key {:?}", secret_key))?,
        noise::SecretKeyField::Absent => None,
    };
    Ok(ValidatedNoiseSpec { secret_key, .. v })
}

fn await_bind_noise(
    ds: &mut Arc<Cap>,
    t: &mut Activation,
    service_selector: AnyValue,
    observer: Arc<Cap>,
    direct_resolution_facet: FacetId,
) -> ActorResult {
    let handler = syndicate::entity(())
        .on_asserted_facet(move |_state, t, a: AnyValue| {
            t.stop_facet(direct_resolution_facet);
            let observer = Arc::clone(&observer);
            t.spawn_link(None, move |t| {
                let bindings = a.value().to_sequence()?;
                let spec = validate_noise_service_spec(language().parse(&bindings[0])?)?;
                let service = bindings[1].value().to_embedded()?.clone();
                let hs = make_handshake(&spec, false)?;
                let responder_session = Cap::guard(crate::Language::arc(), t.create(
                    ResponderState::Introduction{ service, hs }));
                observer.assert(
                    t, language(), &gatekeeper::Resolved::Accepted { responder_session });
                Ok(())
            });
            Ok(())
        })
        .create_cap(t);
    ds.assert(t, language(), &dataspace::Observe {
        // TODO: codegen plugin to generate pattern constructors
        pattern: pattern!{
            <bind <noise $spec:NoiseServiceSpec{ { service: #(&service_selector) } }> $service _>
        },
        observer: handler,
    });
    Ok(())
}

type NoiseHandshakeState = noise_protocol::HandshakeState<X25519, ChaCha20Poly1305, Blake2s>;

struct HandshakeState {
    peer: Arc<Cap>,
    hs: NoiseHandshakeState,
    initial_ref: Option<Arc<Cap>>,
    initial_oid: Option<sturdy::Oid>,
}

struct TransportState {
    relay_input: Arc<Mutex<Option<TunnelRelay>>>,
    c_recv: CipherState<ChaCha20Poly1305>,
}

enum ResponderState {
    Invalid, // used during state transitions
    Introduction {
        service: Arc<Cap>,
        hs: NoiseHandshakeState,
    },
    Handshake(HandshakeState),
    Transport(TransportState),
}

impl Entity<noise::SessionItem> for ResponderState {
    fn assert(&mut self, _t: &mut Activation, item: noise::SessionItem, _handle: Handle) -> ActorResult {
        let initiator_session = match item {
            noise::SessionItem::Initiator(i_box) => i_box.initiator_session,
            noise::SessionItem::Packet(_) => Err("Unexpected Packet assertion")?,
        };
        match std::mem::replace(self, ResponderState::Invalid) {
            ResponderState::Introduction { service, hs } => {
                *self = ResponderState::Handshake(HandshakeState {
                    peer: initiator_session,
                    hs,
                    initial_ref: Some(service.clone()),
                    initial_oid: None,
                });
                Ok(())
            }
            _ =>
                Err("Received second Initiator")?,
        }
    }

    fn message(&mut self, t: &mut Activation, item: noise::SessionItem) -> ActorResult {
        let p = match item {
            noise::SessionItem::Initiator(_) => Err("Unexpected Initiator message")?,
            noise::SessionItem::Packet(p_box) => *p_box,
        };
        match self {
            ResponderState::Invalid | ResponderState::Introduction { .. } =>
                Err("Received Packet in invalid ResponderState")?,
            ResponderState::Handshake(hss) => {
                if let Some((None, ts)) = hss.handle_packet(t, p)? {
                    *self = ResponderState::Transport(ts);
                }
            }
            ResponderState::Transport(ts) => ts.handle_packet(t, p)?,
        }
        Ok(())
    }
}

impl HandshakeState {
    fn handle_packet(
        &mut self,
        t: &mut Activation,
        p: noise::Packet,
    ) -> Result<Option<(Option<Arc<Cap>>, TransportState)>, ActorError> {
        match p {
            noise::Packet::Complete(bs) => {
                if bs.len() < self.hs.get_next_message_overhead() {
                    Err("Invalid handshake message for pattern")?;
                }
                if bs.len() > self.hs.get_next_message_overhead() {
                    Err("Cannot accept payload during handshake")?;
                }
                self.hs.read_message(&bs, &mut [])?;
                if self.hs.completed() {
                    self.complete_handshake(t)
                } else {
                    self.send_handshake_packet(t)
                }
            }
            _ => Err("Fragmented handshake is not allowed")?,
        }
    }

    fn send_handshake_packet(
        &mut self,
        t: &mut Activation,
    ) -> Result<Option<(Option<Arc<Cap>>, TransportState)>, ActorError> {
        let mut reply = vec![0u8; self.hs.get_next_message_overhead()];
        self.hs.write_message(&[], &mut reply[..])?;
        self.peer.message(t, language(), &noise::Packet::Complete(reply.into()));
        if self.hs.completed() {
            self.complete_handshake(t)
        } else {
            Ok(None)
        }
    }

    fn complete_handshake(
        &mut self,
        t: &mut Activation,
    ) -> Result<Option<(Option<Arc<Cap>>, TransportState)>, ActorError> {
        let (c_i_to_r, c_r_to_i) = self.hs.get_ciphers();
        let (c_recv, mut c_send) = if self.hs.get_is_initiator() {
            (c_r_to_i, c_i_to_r)
        } else {
            (c_i_to_r, c_r_to_i)
        };
        let (peer_service, relay_input, mut relay_output) =
            TunnelRelay::_run(t, self.initial_ref.clone(), self.initial_oid.clone(), false);
        let trace_collector = t.trace_collector();
        let peer = self.peer.clone();
        let relay_output_name = Some(AnyValue::symbol("relay_output"));
        let transport_facet = t.facet_ref();
        t.linked_task(relay_output_name.clone(), async move {
            let account = Account::new(relay_output_name, trace_collector);
            let cause = TurnCause::external("relay_output");
            loop {
                match relay_output.recv().await {
                    None => return Ok(LinkedTaskTermination::KeepFacet),
                    Some(loaned_item) => {
                        const MAXSIZE: usize = 65535 - 16; /* Noise tag length is 16 */
                        let p = if loaned_item.item.len() > MAXSIZE {
                            noise::Packet::Fragmented(
                                loaned_item.item
                                    .chunks(MAXSIZE)
                                    .map(|c| c_send.encrypt_vec(c))
                                    .collect())
                        } else {
                            noise::Packet::Complete(c_send.encrypt_vec(&loaned_item.item))
                        };
                        if !transport_facet.activate(&account, Some(cause.clone()), |t| {
                            peer.message(t, language(), &p);
                            Ok(())
                        }) {
                            break;
                        }
                    }
                }
            }
            Ok(LinkedTaskTermination::Normal)
        });
        Ok(Some((peer_service, TransportState { relay_input, c_recv })))
    }
}

impl TransportState {
    fn handle_packet(
        &mut self,
        t: &mut Activation,
        p: noise::Packet,
    ) -> ActorResult {
        let bs = match p {
            noise::Packet::Complete(bs) =>
                self.c_recv.decrypt_vec(&bs[..]).map_err(|_| "Cannot decrypt packet")?,
            noise::Packet::Fragmented(pieces) => {
                let mut result = Vec::with_capacity(1024);
                for piece in pieces {
                    result.extend(self.c_recv.decrypt_vec(&piece[..])
                                  .map_err(|_| "Cannot decrypt packet fragment")?);
                }
                result
            }
        };
        let mut g = self.relay_input.lock();
        let tr = g.as_mut().expect("initialized");
        tr.handle_inbound_datagram(t, &bs[..])?;
        Ok(())
    }
}

fn lookup_pattern(name: &str) -> Option<HandshakePattern> {
    use noise_protocol::patterns::*;
    Some(match name {
        "N" => noise_n(),
        "K" => noise_k(),
        "X" => noise_x(),
        "NN" => noise_nn(),
        "NK" => noise_nk(),
        "NX" => noise_nx(),
        "XN" => noise_xn(),
        "XK" => noise_xk(),
        "XX" => noise_xx(),
        "KN" => noise_kn(),
        "KK" => noise_kk(),
        "KX" => noise_kx(),
        "IN" => noise_in(),
        "IK" => noise_ik(),
        "IX" => noise_ix(),
        "Npsk0" => noise_n_psk0(),
        "Kpsk0" => noise_k_psk0(),
        "Xpsk1" => noise_x_psk1(),
        "NNpsk0" => noise_nn_psk0(),
        "NNpsk2" => noise_nn_psk2(),
        "NKpsk0" => noise_nk_psk0(),
        "NKpsk2" => noise_nk_psk2(),
        "NXpsk2" => noise_nx_psk2(),
        "XNpsk3" => noise_xn_psk3(),
        "XKpsk3" => noise_xk_psk3(),
        "XXpsk3" => noise_xx_psk3(),
        "KNpsk0" => noise_kn_psk0(),
        "KNpsk2" => noise_kn_psk2(),
        "KKpsk0" => noise_kk_psk0(),
        "KKpsk2" => noise_kk_psk2(),
        "KXpsk2" => noise_kx_psk2(),
        "INpsk1" => noise_in_psk1(),
        "INpsk2" => noise_in_psk2(),
        "IKpsk1" => noise_ik_psk1(),
        "IKpsk2" => noise_ik_psk2(),
        "IXpsk2" => noise_ix_psk2(),
        "NNpsk0+psk2" => noise_nn_psk0_psk2(),
        "NXpsk0+psk1+psk2" => noise_nx_psk0_psk1_psk2(),
        "XNpsk1+psk3" => noise_xn_psk1_psk3(),
        "XKpsk0+psk3" => noise_xk_psk0_psk3(),
        "KNpsk1+psk2" => noise_kn_psk1_psk2(),
        "KKpsk0+psk2" => noise_kk_psk0_psk2(),
        "INpsk1+psk2" => noise_in_psk1_psk2(),
        "IKpsk0+psk2" => noise_ik_psk0_psk2(),
        "IXpsk0+psk2" => noise_ix_psk0_psk2(),
        "XXpsk0+psk1" => noise_xx_psk0_psk1(),
        "XXpsk0+psk2" => noise_xx_psk0_psk2(),
        "XXpsk0+psk3" => noise_xx_psk0_psk3(),
        "XXpsk0+psk1+psk2+psk3" => noise_xx_psk0_psk1_psk2_psk3(),
        _ => return None,
    })
}

fn make_handshake(
    spec: &ValidatedNoiseSpec,
    is_initiator: bool,
) -> Result<NoiseHandshakeState, ActorError> {
    let mut builder = noise_protocol::HandshakeStateBuilder::new();
    builder.set_pattern(spec.pattern.clone());
    builder.set_is_initiator(is_initiator);
    let prologue = PackedWriter::encode(&mut NoEmbeddedDomainCodec, &spec.service)?;
    builder.set_prologue(&prologue);
    match spec.secret_key.clone() {
        None => (),
        Some(sk) => {
            let sk: [u8; 32] = sk.try_into().map_err(|_| "Bad secret key length")?;
            builder.set_s(U8Array::from_slice(&sk));
        },
    }
    builder.set_rs(U8Array::from_slice(&spec.public_key));
    let mut hs = builder.build_handshake_state();
    for psk in spec.psks.iter() {
        hs.push_psk(psk);
    }
    Ok(hs)
}

pub fn handle_noise_path_steps(t: &mut Activation, ds: Arc<Cap>) -> ActorResult {
    during!(t, ds, language(),
            <q <resolve-path-step $origin <noise $spec0>>>,
            enclose!((ds) move |t: &mut Activation| {
                if let Ok(spec) = language().parse::<noise::NoiseSpec>(&spec0) {
                    if let Some(origin) = origin.value().as_embedded().cloned() {
                        t.spawn_link(None, move |t| run_noise_initiator(t, ds, origin, spec));
                    }
                }
                Ok(())
            }));
    Ok(())
}

fn run_noise_initiator(
    t: &mut Activation,
    ds: Arc<Cap>,
    origin: Arc<Cap>,
    spec: noise::NoiseSpec,
) -> ActorResult {
    let q = language().unparse(&gatekeeper::ResolvePathStep {
        origin: origin.clone(),
        path_step: gatekeeper::PathStep {
            step_type: "noise".to_string(),
            detail: language().unparse(&spec),
        }
    });
    let service = spec.service.clone();
    let validated = validate_noise_spec(spec)?;
    let observer = Cap::guard(&language().syndicate, t.create(
        syndicate::entity(()).on_asserted_facet(
            enclose!((ds, q) move |_, t, r: gatekeeper::Resolved| {
                match r {
                    gatekeeper::Resolved::Rejected(b) => {
                        ds.assert(t, language(), &rpc::answer(
                            language(), q.clone(), R::Result::Error {
                                error: b.detail }));
                    }
                    gatekeeper::Resolved::Accepted { responder_session } =>
                        run_initiator_session(
                            t, ds.clone(), q.clone(), &validated, responder_session)?,
                }
                Ok(())
            }))));
    origin.assert(t, language(), &gatekeeper::Resolve {
        step: gatekeeper::Step {
            step_type: "noise".to_string(),
            detail: language().unparse(&service),
        },
        observer,
    });
    Ok(())
}

fn run_initiator_session(
    t: &mut Activation,
    ds: Arc<Cap>,
    question: AnyValue,
    spec: &ValidatedNoiseSpec,
    responder_session: Arc<Cap>,
) -> ActorResult {
    let initiator_session_ref = t.create_inert();
    let initiator_session = Cap::guard(crate::Language::arc(), initiator_session_ref.clone());
    responder_session.assert(t, language(), &noise::Initiator { initiator_session });
    let mut hss = HandshakeState {
        peer: responder_session.clone(),
        hs: make_handshake(spec, true)?,
        initial_ref: None,
        initial_oid: Some(sturdy::Oid(0.into())),
    };
    if !hss.hs.completed() {
        if hss.send_handshake_packet(t)?.is_some() {
            // TODO: this might be a valid pattern, check
            panic!("Unexpected complete handshake after no messages");
        }
    }
    initiator_session_ref.become_entity(InitiatorState::Handshake { ds, question, hss });
    Ok(())
}

enum InitiatorState {
    Handshake {
        ds: Arc<Cap>,
        question: AnyValue,
        hss: HandshakeState,
    },
    Transport(TransportState),
}

impl Entity<noise::Packet> for InitiatorState {
    fn message(&mut self, t: &mut Activation, p: noise::Packet) -> ActorResult {
        match self {
            InitiatorState::Handshake { hss, ds, question } => {
                if let Some((Some(peer_service), ts)) = hss.handle_packet(t, p)? {
                    let ds = ds.clone();
                    let question = question.clone();
                    *self = InitiatorState::Transport(ts);
                    ds.assert(t, language(), &rpc::answer(language(), question, R::Result::Ok {
                        value: AnyValue::domain(peer_service) }));
                }
            }
            InitiatorState::Transport(ts) => ts.handle_packet(t, p)?,
        }
        Ok(())
    }
}
