//! The `sysinfo` handle shared by the collectors that sample the host.
//!
//! Since `sysinfo` 0.40 creating a `System` can fail: it reports `Unsupported` on a
//! platform `sysinfo` has no backend for, and an I/O error where the platform's own
//! query fails. It cannot fail on Linux. Collectors are constructed infallibly at
//! startup and must not panic, so they hold a [`LazySystem`] and create the handle when
//! they first sample.

use sysinfo::System;
use tracing::warn;

/// A `sysinfo::System` created on first use.
///
/// A failure to create it is logged once and tried again on the next scrape, so a
/// transient failure heals and a permanent one does not flood the log. While there is no
/// handle the caller publishes nothing from it.
pub(crate) struct LazySystem {
    system: Option<System>,
    warned: bool,
}

impl LazySystem {
    pub(crate) const fn new() -> Self {
        Self {
            system: None,
            warned: false,
        }
    }

    /// Returns the handle, creating it if this is the first use.
    ///
    /// `None` means it could not be created; `collector` names the caller in the log.
    pub(crate) fn get(&mut self, collector: &'static str) -> Option<&mut System> {
        self.get_or_create(collector, System::new)
    }

    fn get_or_create(
        &mut self,
        collector: &'static str,
        create: impl FnOnce() -> Result<System, sysinfo::Error>,
    ) -> Option<&mut System> {
        if self.system.is_none() {
            match create() {
                Ok(system) => self.system = Some(system),
                Err(error) => {
                    if !self.warned {
                        self.warned = true;
                        warn!(
                            collector,
                            error = %error,
                            "host system information is unavailable; its metrics are not updated"
                        );
                    }
                    return None;
                }
            }
        }
        self.system.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::LazySystem;
    use sysinfo::System;

    #[test]
    fn creates_the_handle_once_and_reuses_it() {
        let mut lazy = LazySystem::new();
        let mut created = 0;

        for _ in 0..3 {
            let handle = lazy.get_or_create("test", || {
                created += 1;
                System::new()
            });
            assert!(handle.is_some(), "a supported platform yields a handle");
        }

        assert_eq!(created, 1, "the handle is created on first use only");
    }

    #[test]
    fn a_failed_creation_yields_nothing_and_is_retried() {
        let mut lazy = LazySystem::new();

        let first = lazy.get_or_create("test", || Err(sysinfo::Error::Unsupported));
        assert!(first.is_none(), "no handle while creation fails");
        assert!(lazy.warned, "the failure is reported");

        let second = lazy.get_or_create("test", || Err(sysinfo::Error::Unsupported));
        assert!(second.is_none(), "still no handle on a repeated failure");

        let third = lazy.get_or_create("test", System::new);
        assert!(
            third.is_some(),
            "a later scrape recovers once creation works"
        );
    }

    #[test]
    fn the_real_constructor_works_on_this_platform() {
        let mut lazy = LazySystem::new();
        assert!(lazy.get("test").is_some());
        assert!(!lazy.warned);
    }
}
