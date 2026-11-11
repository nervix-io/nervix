//! Explicit trust changes served by the scenario's signed HTTPS peer.
//!
//! Layer: test harness.
//! - **Owns.** Root-authenticated revocation, working-key replacement and epoch succession scripts.
//! - **Depends on.** Shared trust document types and the bounded peer's real signers.
//! - **Must not know.** Cluster trust storage, policy or scenario assertions.

use super::*;

impl Model {
    fn trust_document<T: Document>(
        &mut self,
        document: &T,
    ) -> error_stack::Result<(), ServiceError> {
        assert!(self.documents.documents.len() < MAX_ENTRIES);
        let mut signatures = Vec::new();
        for root in &self.roots[..2] {
            signatures.push(
                root.sign(document, limits())
                    .change_context(ServiceError::Model)?,
            );
        }
        self.documents
            .documents
            .push(SignedTrustDocument::Signatures(SignatureSet { signatures }));
        Ok(())
    }

    fn trust_serial(&self) -> u64 {
        u64::try_from(self.documents.documents.len()).assured("bounded trust inventory fits u64")
            + 1
    }

    pub fn revoke_working_key(&mut self) -> error_stack::Result<(), ServiceError> {
        self.trust_document(&WorkingKeyRevocation {
            doc: RevocationKind::WorkingKeyRevocation,
            protocol: 1,
            serial: self.trust_serial(),
            issued_at: self.now,
            root_epoch: self.root_epoch,
            kid: self.working.key_id().clone(),
            revoked_at: self.now,
            reason: "scenario administrator revoked the working key".to_owned(),
        })
    }

    pub fn replace_working_key(&mut self) -> error_stack::Result<(), ServiceError> {
        let working = signer()?;
        self.trust_document(&WorkingKeyAuthorization {
            doc: AuthorizationKind::WorkingKeyAuthorization,
            protocol: 1,
            serial: self.trust_serial(),
            issued_at: self.now,
            root_epoch: self.root_epoch,
            kid: working.key_id().clone(),
            public_jwk: working.public_key().clone(),
            uses: vec![WorkingKeyUse::Decision],
            not_before: self
                .now
                .checked_sub_signed(TimeDelta::days(365))
                .assured("bounded fixture clock"),
            not_after: self
                .now
                .checked_add_signed(TimeDelta::days(365))
                .assured("bounded fixture clock"),
        })?;
        self.working = working;
        Ok(())
    }

    pub fn succeed_roots(
        &mut self,
        provide_succession: bool,
    ) -> error_stack::Result<(), ServiceError> {
        let roots = [signer()?, signer()?, signer()?];
        let next = self
            .root_epoch
            .checked_add(1)
            .assured("bounded fixture epoch");
        if provide_succession {
            self.trust_document(&RootSuccession {
                doc: SuccessionKind::RootSuccession,
                protocol: 1,
                serial: self.trust_serial(),
                issued_at: self.now,
                from_epoch: self.root_epoch,
                to_epoch: next,
                roots: roots.iter().map(|root| root.public_key().clone()).collect(),
                threshold: std::num::NonZeroUsize::new(2).assured("two-root threshold"),
                effective_at: self.now,
            })?;
        }
        self.roots = roots;
        self.root_epoch = next;
        self.replace_working_key()
    }
}
