use noise_protocol::CipherState;
use noise_protocol::U8Array;
use noise_protocol::patterns::HandshakePattern;
use noise_rust_crypto::Blake2s;
use noise_rust_crypto::ChaCha20Poly1305;
use noise_rust_crypto::X25519;
use preserves_schema::Codec;
use syndicate::relay::Mutex;
use syndicate::relay::TunnelRelay;
use syndicate::trace::TurnCause;
use syndicate::value::NoEmbeddedDomainCodec;
use syndicate::value::packed::PackedWriter;

use std::convert::TryInto;
use std::sync::Arc;

use syndicate::actor::*;
use syndicate::during::DuringResult;
use syndicate::value::NestedValue;
use syndicate::schemas::dataspace;
use syndicate::schemas::gatekeeper;
use syndicate::schemas::noise;

use crate::language::language;
use crate::schemas::gatekeeper_mux::Api;
use crate::schemas::gatekeeper_mux::NoiseServiceSpec;
use crate::schemas::gatekeeper_mux::SecretKeyField;

// pub fn bind(
//     t: &mut Activation,
//     ds: &Arc<Cap>,
//     oid: syndicate::schemas::sturdy::_Any,
//     key: [u8; 16],
//     target: Arc<Cap>,
// ) {
//     let sr = sturdy::SturdyRef::mint(oid.clone(), &key);
//     tracing::info!(cap = ?language().unparse(&sr), hex = %sr.to_hex());
//     ds.assert(t, language(), &gatekeeper::Bind { oid, key: key.to_vec(), target });
// }

pub fn handle_assertion(
    ds: &mut Arc<Cap>,
    t: &mut Activation,
    a: Api<AnyValue>,
) -> DuringResult<Arc<Cap>> {
    match a {
        Api::Resolve(resolve_box) => handle_resolve(ds, t, *resolve_box),
        Api::Connect(connect_box) => handle_connect(ds, t, *connect_box),
    }
}

fn handle_resolve(
    ds: &mut Arc<Cap>,
    t: &mut Activation,
    a: gatekeeper::Resolve,
) -> DuringResult<Arc<Cap>> {
    let gatekeeper::Resolve { sturdyref, observer } = a;
    let queried_oid = sturdyref.oid.clone();
    let handler = syndicate::entity(observer)
        .on_asserted(move |observer, t, a: AnyValue| {
            let bindings = a.value().to_sequence()?;
            let key = bindings[0].value().to_bytestring()?;
            let unattenuated_target = bindings[1].value().to_embedded()?;
            match sturdyref.validate_and_attenuate(key, unattenuated_target) {
                Err(e) => {
                    tracing::warn!(sturdyref = ?language().unparse(&sturdyref),
                                   "sturdyref failed validation: {}", e);
                    Ok(None)
                },
                Ok(target) => {
                    tracing::trace!(sturdyref = ?language().unparse(&sturdyref),
                                    ?target,
                                    "sturdyref resolved");
                    if let Some(h) = observer.assert(t, &(), &AnyValue::domain(target)) {
                        Ok(Some(Box::new(move |_observer, t| Ok(t.retract(h)))))
                    } else {
                        Ok(None)
                    }
                }
            }
        })
        .create_cap(t);
    if let Some(oh) = ds.assert(t, language(), &dataspace::Observe {
        // TODO: codegen plugin to generate pattern constructors
        pattern: syndicate_macros::pattern!{<bind #(&queried_oid) $ $>},
        observer: handler,
    }) {
        Ok(Some(Box::new(move |_ds, t| Ok(t.retract(oh)))))
    } else {
        Ok(None)
    }
}

fn handle_connect(
    ds: &mut Arc<Cap>,
    t: &mut Activation,
    a: noise::Connect<AnyValue>,
) -> DuringResult<Arc<Cap>> {
    let noise::Connect { service_selector, initiator_session } = a;
    let handler = syndicate::entity(())
        .on_asserted_facet(move |_state, t, a: AnyValue| {
            let initiator_session = Arc::clone(&initiator_session);
            t.spawn_link(None, move |t| {
                let bindings = a.value().to_sequence()?;
                let spec: NoiseServiceSpec<AnyValue> = language().parse(&bindings[0])?;
                let protocol = match spec.base.protocol {
                    noise::NoiseProtocol::Present { protocol } =>
                        protocol,
                    noise::NoiseProtocol::Invalid { protocol } =>
                        Err(format!("Invalid noise protocol {:?}", protocol))?,
                    noise::NoiseProtocol::Absent =>
                        language().unparse(&noise::DefaultProtocol).value().to_string()?.clone(),
                };
                let psks = match spec.base.pre_shared_keys {
                    noise::NoisePreSharedKeys::Present { pre_shared_keys } =>
                        pre_shared_keys,
                    noise::NoisePreSharedKeys::Invalid { pre_shared_keys } =>
                        Err(format!("Invalid pre-shared-keys {:?}", pre_shared_keys))?,
                    noise::NoisePreSharedKeys::Absent =>
                        vec![],
                };
                let secret_key = match spec.secret_key {
                    SecretKeyField::Present { secret_key } =>
                        Some(secret_key),
                    SecretKeyField::Invalid { secret_key } =>
                        Err(format!("Invalid secret key {:?}", secret_key))?,
                    SecretKeyField::Absent =>
                        None,
                };
                let service = bindings[1].value().to_embedded()?;
                run_noise_responder(t,
                                    spec.base.service,
                                    protocol,
                                    psks,
                                    secret_key,
                                    initiator_session,
                                    Arc::clone(service))
            });
            Ok(())
        })
        .create_cap(t);
    if let Some(oh) = ds.assert(t, language(), &dataspace::Observe {
        // TODO: codegen plugin to generate pattern constructors
        pattern: syndicate_macros::pattern!{
            <noise $spec:NoiseServiceSpec{ { service: #(&service_selector) } } $service >
        },
        observer: handler,
    }) {
        Ok(Some(Box::new(move |_ds, t| Ok(t.retract(oh)))))
    } else {
        Ok(None)
    }
}

struct ResponderDetails {
    initiator_session: Arc<Cap>,
    service: Arc<Cap>,
}

struct ResponderTransport {
    relay_input: Arc<Mutex<Option<TunnelRelay>>>,
    c_recv: CipherState<ChaCha20Poly1305>
}

enum ResponderState {
    Handshake(ResponderDetails, noise_protocol::HandshakeState<X25519, ChaCha20Poly1305, Blake2s>),
    Transport(ResponderTransport),
}

impl Entity<noise::Packet> for ResponderState {
    fn message(&mut self, t: &mut Activation, p: noise::Packet) -> ActorResult {
        match self {
            ResponderState::Handshake(details, hs) => match p {
                noise::Packet::Complete(bs) => {
                    if bs.len() < hs.get_next_message_overhead() {
                        Err("Invalid handshake message for pattern")?;
                    }
                    if bs.len() > hs.get_next_message_overhead() {
                        Err("Cannot accept payload during handshake")?;
                    }
                    hs.read_message(&bs, &mut [])?;
                    let mut reply = vec![0u8; hs.get_next_message_overhead()];
                    hs.write_message(&[], &mut reply[..])?;
                    details.initiator_session.message(t, language(), &noise::Packet::Complete(reply.into()));
                    if hs.completed() {
                        let (c_recv, mut c_send) = hs.get_ciphers();
                        let (_, relay_input, mut relay_output) =
                            TunnelRelay::_run(t, Some(Arc::clone(&details.service)), None, false);
                        let trace_collector = t.trace_collector();
                        let transport = ResponderTransport { relay_input, c_recv };
                        let initiator_session = Arc::clone(&details.initiator_session);
                        let relay_output_name = Some(AnyValue::symbol("relay_output"));
                        let transport_facet = t.facet.clone();
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
                                            initiator_session.message(t, language(), &p);
                                            Ok(())
                                        }) {
                                            break;
                                        }
                                    }
                                }
                            }
                            Ok(LinkedTaskTermination::Normal)
                        });
                        *self = ResponderState::Transport(transport);
                    }
                }
                _ => Err("Fragmented handshake is not allowed")?,
            },
            ResponderState::Transport(transport) => {
                let bs = match p {
                    noise::Packet::Complete(bs) =>
                        transport.c_recv.decrypt_vec(&bs[..]).map_err(|_| "Cannot decrypt packet")?,
                    noise::Packet::Fragmented(pieces) => {
                        let mut result = Vec::with_capacity(1024);
                        for piece in pieces {
                            result.extend(transport.c_recv.decrypt_vec(&piece[..])
                                .map_err(|_| "Cannot decrypt packet fragment")?);
                        }
                        result
                    }
                };
                let mut g = transport.relay_input.lock();
                let tr = g.as_mut().expect("initialized");
                tr.handle_inbound_datagram(t, &bs[..])?;
            }
        }
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

fn run_noise_responder(
    t: &mut Activation,
    service_selector: AnyValue,
    protocol: String,
    psks: Vec<Vec<u8>>,
    secret_key: Option<Vec<u8>>,
    initiator_session: Arc<Cap>,
    service: Arc<Cap>,
) -> ActorResult {
    const PREFIX: &'static str = "Noise_";
    const SUFFIX: &'static str = "_25519_ChaChaPoly_BLAKE2s";
    if !protocol.starts_with(PREFIX) || !protocol.ends_with(SUFFIX) {
        Err(format!("Unsupported protocol {:?}", protocol))?;
    }
    let pattern_name = &protocol[PREFIX.len()..(protocol.len()-SUFFIX.len())];
    let pattern = lookup_pattern(pattern_name).ok_or_else::<ActorError, _>(
        || format!("Unsupported handshake pattern {:?}", pattern_name).into())?;

    let hs = {
        let mut builder = noise_protocol::HandshakeStateBuilder::new();
        builder.set_pattern(pattern);
        builder.set_is_initiator(false);
        let prologue = PackedWriter::encode(&mut NoEmbeddedDomainCodec, &service_selector)?;
        builder.set_prologue(&prologue);
        match secret_key {
            None => (),
            Some(sk) => {
                let sk: [u8; 32] = sk.try_into().map_err(|_| "Bad secret key length")?;
                builder.set_s(U8Array::from_slice(&sk));
            },
        }
        let mut hs = builder.build_handshake_state();
        for psk in psks.into_iter() {
            hs.push_psk(&psk);
        }
        hs
    };

    let details = ResponderDetails {
        initiator_session: initiator_session.clone(),
        service,
    };

    let responder_session =
        Cap::guard(crate::Language::arc(), t.create(ResponderState::Handshake(details, hs)));
    initiator_session.assert(t, language(), &noise::Accept { responder_session });
    Ok(())
}
