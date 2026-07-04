use iotmq::Server;

fn main() {
    Server::new().run();
    loop {}
}
