//! Controlled failures of the production resource archive writer.

use std::{
    collections::BTreeMap,
    io::{self, Read as _},
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

use meticulous::ResultExt as _;
use tokio::io::{AsyncReadExt as _, AsyncWrite};

use super::UploadArchive;

#[nervix_primitives::test]
async fn cancelling_an_archive_write_releases_its_writer() {
    let directory = tempfile::tempdir().assured("the test can create a directory");
    std::fs::write(directory.path().join("model.bin"), [7; 1024])
        .assured("the test can create its resource file");
    let path = directory.path().to_path_buf();
    let (writer, mut reader) = tokio::io::duplex(1);
    let writing =
        nervix_primitives::task::spawn(async move { UploadArchive::write(&path, writer).await });
    reader
        .read_u8()
        .await
        .assured("the production archive writer has begun writing its header");

    writing.abort();
    let ended = writing.await.expect_err("the archive write was cancelled");
    assert!(
        ended.is_cancelled(),
        "cancellation must stay cancellation: {ended}"
    );
    let mut remainder = Vec::new();
    reader
        .read_to_end(&mut remainder)
        .await
        .assured("abandoning the archive releases its writer");
}

struct FailingWriter {
    failed: bool,
}

impl AsyncWrite for FailingWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let error = if self.failed {
            io::Error::other("a subsequent write cannot replace the first failure")
        } else {
            self.failed = true;
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the originating write failure",
            )
        };
        Poll::Ready(Err(error))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[nervix_primitives::test]
async fn an_archive_write_keeps_its_originating_io_failure() {
    let directory = tempfile::tempdir().assured("the test can create a directory");
    std::fs::create_dir(directory.path().join("weights"))
        .assured("the test can create an archive entry");
    let error = UploadArchive::write(directory.path(), FailingWriter { failed: false })
        .await
        .expect_err("the archive writer refuses its first write");
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(error.to_string(), "the originating write failure");
}

struct RemovingWriter {
    file: PathBuf,
}

impl AsyncWrite for RemovingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.file.exists() {
            std::fs::remove_file(&self.file).assured("the test removes the collected file");
        }
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[nervix_primitives::test]
async fn a_file_that_disappears_during_archiving_returns_its_io_failure() {
    let directory = tempfile::tempdir().assured("the test can create a directory");
    std::fs::create_dir(directory.path().join("a"))
        .assured("the directory is written before the file is opened");
    let file = directory.path().join("b");
    std::fs::write(&file, [7]).assured("the test can create its resource file");
    let error = UploadArchive::write(directory.path(), RemovingWriter { file })
        .await
        .expect_err("the collected file disappeared before it was opened");
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

#[derive(Debug, bolero::TypeGenerator)]
struct ArchiveFiles {
    #[generator(bolero::generator::produce_with::<Vec<u8>>().len(0_usize..=1025))]
    first: Vec<u8>,
    #[generator(bolero::generator::produce_with::<Vec<u8>>().len(0_usize..=1025))]
    second: Vec<u8>,
    long_name: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum ArchiveContent {
    Directory,
    File(Vec<u8>),
}

#[test]
fn bolero_upload_archives_preserve_the_complete_tree_and_deterministic_bytes() {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the archive property can start its runtime");
    bolero::check!()
        .with_iterations(128)
        .with_max_len(4096)
        .with_type::<ArchiveFiles>()
        .for_each(|files| {
            let directory = tempfile::tempdir().assured("the property can create a directory");
            let name = if files.long_name {
                "layer".repeat(30)
            } else {
                "layer".to_string()
            };
            let nested = PathBuf::from("weights").join(name);
            std::fs::create_dir(directory.path().join("weights"))
                .assured("the property can create its nested directory");
            std::fs::create_dir(directory.path().join("empty"))
                .assured("the property can create its empty directory");
            std::fs::write(directory.path().join(&nested), &files.second)
                .assured("the property can create the later file first");
            std::fs::write(directory.path().join("model.bin"), &files.first)
                .assured("the property can create its first file last");
            let archive = runtime
                .block_on(UploadArchive::build(directory.path()))
                .assured("a current resource directory can be archived");
            let bytes = std::fs::read(&archive.path).assured("the completed archive is readable");
            assert_eq!(
                archive.total_bytes.get(),
                u64::try_from(bytes.len()).assured("the bounded archive size fits u64")
            );
            let repeated = runtime
                .block_on(UploadArchive::build(directory.path()))
                .assured("the same directory can be archived again");
            assert_eq!(
                bytes,
                std::fs::read(&repeated.path).assured("the repeated archive is readable")
            );

            let expected = BTreeMap::from([
                (PathBuf::from("empty"), ArchiveContent::Directory),
                (
                    PathBuf::from("model.bin"),
                    ArchiveContent::File(files.first.clone()),
                ),
                (PathBuf::from("weights"), ArchiveContent::Directory),
                (nested, ArchiveContent::File(files.second.clone())),
            ]);
            let mut decoded = tar::Archive::new(bytes.as_slice());
            let mut actual = BTreeMap::new();
            let mut paths = Vec::new();
            for entry in decoded
                .entries()
                .assured("the independent decoder accepts the archive")
            {
                let mut entry = entry.assured("every archive entry is complete");
                let path = entry.path().assured("the entry path is valid").into_owned();
                paths.push(path.clone());
                assert_eq!(entry.header().uid().assured("the owner is encoded"), 0);
                assert_eq!(entry.header().gid().assured("the group is encoded"), 0);
                assert_eq!(entry.header().mtime().assured("the time is encoded"), 0);
                let contents = if entry.header().entry_type().is_dir() {
                    assert_eq!(
                        entry
                            .header()
                            .mode()
                            .assured("the directory mode is encoded"),
                        0o755
                    );
                    ArchiveContent::Directory
                } else {
                    assert!(entry.header().entry_type().is_file());
                    assert_eq!(
                        entry.header().mode().assured("the file mode is encoded"),
                        0o644
                    );
                    let mut contents = Vec::new();
                    entry
                        .read_to_end(&mut contents)
                        .assured("the complete file is readable");
                    ArchiveContent::File(contents)
                };
                assert!(
                    actual.insert(path, contents).is_none(),
                    "entries are unique"
                );
            }
            assert_eq!(paths, expected.keys().cloned().collect::<Vec<_>>());
            assert_eq!(actual, expected);
        });
}
