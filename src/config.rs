use structopt::StructOpt;

#[derive(Clone, StructOpt)]
pub struct ServerConfig {
    #[structopt(short = "p", long = "port", default_value = "8001")]
    pub ports: Vec<u16>,

    #[structopt(long, default_value = "10000")]
    pub overload_threshold: usize,
    #[structopt(long, default_value = "5")]
    pub overload_turn_limit: usize,
}
