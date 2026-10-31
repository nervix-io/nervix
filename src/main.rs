use error_stack::Report;
use nervix_server::application::{AppError, run_server_process};

nervix_primitives::product_binary!("nervix-server", diagnostic);

fn main() -> Result<(), Report<AppError>> {
    run_server_process()
}
