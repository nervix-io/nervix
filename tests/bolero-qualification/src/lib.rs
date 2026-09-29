//! Deliberately failing targets used only to qualify the fuzz runner.

#[cfg(test)]
mod tests {
    #[test]
    fn bolero_qualification_fails_on_marker() {
        bolero::check!()
            .with_iterations(0)
            .with_max_len(16)
            .for_each(|bytes: &[u8]| {
                assert!(!bytes.contains(&0x42), "qualification marker");
            });
    }

    #[test]
    fn bolero_qualification_times_out_on_marker() {
        bolero::check!()
            .with_iterations(0)
            .with_max_len(16)
            .for_each(|bytes: &[u8]| {
                if bytes.contains(&0x55) {
                    loop {
                        std::hint::spin_loop();
                    }
                }
            });
    }
}
