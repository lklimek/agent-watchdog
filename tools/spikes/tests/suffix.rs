use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};

use agent_watchdog_phase0_probes::{SuffixRead, read_bounded_suffix};

const EIGHT_GIB: u64 = 8 * 1024 * 1024 * 1024;

#[test]
fn huge_sparse_transcript_reads_only_appended_suffix() {
    let file = tempfile::NamedTempFile::new().expect("temporary transcript should exist");
    file.as_file()
        .set_len(EIGHT_GIB)
        .expect("sparse transcript should resize");
    let mut writer = OpenOptions::new()
        .append(true)
        .open(file.path())
        .expect("transcript should reopen");
    let record = b"{\"session\":\"phase0\",\"state\":\"running\"}\n";
    writer.write_all(record).expect("record should append");

    let result = read_bounded_suffix(file.path(), EIGHT_GIB, 64 * 1024)
        .expect("appended suffix should be readable");

    assert_eq!(
        result,
        SuffixRead::Appended {
            bytes: record.to_vec(),
            next_offset: EIGHT_GIB + record.len() as u64,
            at_eof: true,
        }
    );
}

#[test]
fn suffix_read_respects_byte_budget() {
    let mut file = tempfile::NamedTempFile::new().expect("temporary transcript should exist");
    file.write_all(b"0123456789").expect("fixture should write");
    file.as_file_mut()
        .seek(SeekFrom::Start(0))
        .expect("fixture should rewind");

    let result = read_bounded_suffix(file.path(), 0, 4).expect("suffix should be readable");

    assert_eq!(
        result,
        SuffixRead::Appended {
            bytes: b"0123".to_vec(),
            next_offset: 4,
            at_eof: false,
        }
    );
}

#[test]
fn cursor_past_end_reports_truncation_without_reading_from_zero() {
    let mut file = tempfile::NamedTempFile::new().expect("temporary transcript should exist");
    file.write_all(b"short").expect("fixture should write");

    let result = read_bounded_suffix(file.path(), 100, 64).expect("metadata should be readable");

    assert_eq!(result, SuffixRead::Truncated { file_len: 5 });
}
