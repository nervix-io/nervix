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
    fn bolero_qualification_reports_memory_error_on_marker() {
        bolero::check!()
            .with_iterations(0)
            .with_max_len(16)
            .for_each(|bytes: &[u8]| {
                if bytes.contains(&0x33) {
                    let allocation = vec![0_u8; 4];
                    let past_end = allocation.as_ptr().wrapping_add(allocation.len());
                    // SAFETY: none. This reads one byte past the allocation on purpose, so that
                    // AddressSanitizer reports a heap-buffer-overflow and the runner must fail.
                    let byte = unsafe { past_end.read_volatile() };
                    std::hint::black_box(byte);
                }
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
