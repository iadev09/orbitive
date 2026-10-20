use crate::{CasBudget, Error};

pub(crate) struct WorkBudget {
    pub(crate) remaining: usize,
}

impl WorkBudget {
    pub(crate) fn new(limit: CasBudget) -> Self {
        Self { remaining: limit.attempts().get() }
    }

    pub(crate) fn take(&mut self) -> Result<(), Error> {
        self.remaining = self.remaining.checked_sub(1).ok_or(Error::Busy)?;
        Ok(())
    }
}
