pub struct Server {}

impl Server {
    pub fn new() -> Self {
        Self {}
    }
    pub fn run(&mut self) {
        println!("run server");
        let ctx = crate::context::Context::new();
        let ctx1 = ctx.clone();
        println!("{:?}", ctx.config.read());
    }
}
