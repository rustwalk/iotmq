use iotmq::Server;

fn main() {
    if let Err(e) = Server::start() {
        eprintln!("{}", e);
    }
}
