//! Ruby's typed Arrow batches and per-level builders.
//!
//! Layer: edges.
//! - **Owns.** Typed/bulk column access, canonical IPC views and copied builder inputs.
//! - **Depends on.** The shared host batch/builder, exact schemas and Ruby value projection.
//! - **Must not know.** Producers, deliveries, sessions or how Arrow IPC is encoded.

use std::cell::RefCell;

use magnus::{
    DataTypeFunctions, Error, RArray, RModule, RString, Ruby, TypedData, Value, function, method,
    prelude::*, typed_data::Obj,
};
use meticulous::ResultExt as _;
use nervix_client_host as host;
use nervix_primitives::sync::Arc;

use crate::{errors, gvl, schema::Schema, values};

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::Batch", free_immediately)]
pub(crate) struct Batch {
    pub(crate) batch: Arc<host::Batch>,
}

impl Batch {
    pub(crate) fn new(batch: Arc<host::Batch>) -> Self {
        Self { batch }
    }

    fn decode(schema: &Schema, ipc: RString, rows: u32) -> Result<Self, Error> {
        let bytes = bytes::Bytes::from(values::bytes(ipc));
        let schema = schema.schema.clone();
        let batch = gvl::without_gvl(None, |_| host::Batch::decode(bytes, schema, rows))?;
        Ok(Self::new(Arc::new(batch)))
    }
    fn row_count(&self) -> usize {
        self.batch.row_count()
    }
    fn schema(&self) -> Schema {
        Schema::new(self.batch.schema().clone())
    }
    fn ipc(this: Obj<Self>) -> Result<Value, Error> {
        let batch = this.batch.clone();
        let bytes = gvl::without_gvl(None, |_| batch.ipc())?;
        // SAFETY: Batch is immutable and has no release operation. Its host batch keeps the
        // cached stream until this exact Ruby object, retained by the buffer, is collected.
        unsafe { values::buffer(this.as_value(), bytes) }
    }
    fn column(&self, index: usize) -> Result<RArray, Error> {
        let ruby = Ruby::get().assured("a native Ruby method holds the GVL");
        let field = self
            .batch
            .schema()
            .field(host::Part::Rows, index)
            .map_err(errors::raise)?;
        let array = self.batch.record_batch().column(index);
        let values = ruby.ary_new_capa(array.len());
        for row in 0..array.len() {
            values.push(values::arrow(&ruby, array.as_ref(), row, &field.ty)?)?;
        }
        Ok(values)
    }
    fn cells(&self, column: usize, level: usize) -> Result<usize, Error> {
        self.batch.cells(column, level).map_err(errors::raise)
    }
    fn states(&self, column: usize) -> Result<Vec<u8>, Error> {
        let mut states = vec![0; self.batch.row_count()];
        self.batch
            .states(column, &mut states)
            .map_err(errors::raise)?;
        Ok(states)
    }
    fn offsets(&self, column: usize, level: usize) -> Result<Vec<u64>, Error> {
        let cells = self.cells(column, level)?;
        let ends = cells
            .checked_add(1)
            .ok_or_else(|| errors::invalid("level", "overflows"))?;
        let mut offsets = vec![0; ends];
        self.batch
            .offsets(column, level, &mut offsets)
            .map_err(errors::raise)?;
        Ok(offsets)
    }
    fn fixed(&self, column: usize, level: usize) -> Result<RString, Error> {
        let len = self.batch.fixed_len(column, level).map_err(errors::raise)?;
        // SAFETY: The GVL is held and the immutable host accessor only fills output bytes.
        unsafe { values::binary(len, |output| self.batch.fixed(column, level, output)) }
    }
    fn varlen(&self, column: usize, level: usize) -> Result<(Vec<u64>, RString), Error> {
        let len = self
            .batch
            .varlen_len(column, level)
            .map_err(errors::raise)?;
        let cells = self.cells(column, level)?;
        let ends = cells
            .checked_add(1)
            .ok_or_else(|| errors::invalid("level", "overflows"))?;
        let mut offsets = vec![0; ends];
        // SAFETY: The host copies synchronously with no Ruby call while the GVL is held.
        let bytes = unsafe {
            values::binary(len, |output| {
                self.batch.varlen(column, level, &mut offsets, output)
            })
        }?;
        Ok((offsets, bytes))
    }
}

#[derive(DataTypeFunctions, TypedData)]
#[magnus(class = "Nervix::Client::BatchBuilder", free_immediately)]
struct BatchBuilder {
    builder: RefCell<host::BatchBuilder>,
}

impl BatchBuilder {
    fn new(schema: &Schema, rows: usize) -> Self {
        Self {
            builder: RefCell::new(host::BatchBuilder::new(schema.schema.clone(), rows)),
        }
    }
    fn states(&self, column: usize, presence: Vec<bool>) -> Result<(), Error> {
        let mut builder = self
            .builder
            .try_borrow_mut()
            .map_err(|_| errors::borrowed("batch builder"))?;
        builder.presence(column, &presence).map_err(errors::raise)
    }
    fn offsets(&self, column: usize, level: usize, offsets: Vec<u64>) -> Result<(), Error> {
        let mut builder = self
            .builder
            .try_borrow_mut()
            .map_err(|_| errors::borrowed("batch builder"))?;
        builder
            .offsets(column, level, &offsets)
            .map_err(errors::raise)
    }
    fn fixed(&self, column: usize, level: usize, data: RString) -> Result<(), Error> {
        let result = {
            let mut builder = self
                .builder
                .try_borrow_mut()
                .map_err(|_| errors::borrowed("batch builder"))?;
            // SAFETY: The GVL and call frame keep this argument unchanged and alive. The host
            // copies it synchronously without calling Ruby; error projection follows the borrow.
            let bytes = unsafe { data.as_slice() };
            builder.fixed(column, level, bytes)
        };
        result.map_err(errors::raise)
    }
    fn varlen(
        &self,
        column: usize,
        level: usize,
        offsets: Vec<u64>,
        data: RString,
    ) -> Result<(), Error> {
        let result = {
            let mut builder = self
                .builder
                .try_borrow_mut()
                .map_err(|_| errors::borrowed("batch builder"))?;
            // SAFETY: As above, the host copies the borrowed bytes while the GVL is held and
            // makes no Ruby call. The borrow ends before an error can allocate a Ruby exception.
            let bytes = unsafe { data.as_slice() };
            builder.varlen(column, level, &offsets, bytes)
        };
        result.map_err(errors::raise)
    }
    fn finish(&self) -> Result<Batch, Error> {
        let batch = {
            let mut builder = self
                .builder
                .try_borrow_mut()
                .map_err(|_| errors::borrowed("batch builder"))?;
            builder.finish().map_err(errors::raise)?
        };
        Ok(Batch::new(Arc::new(batch)))
    }
}

pub(crate) fn register(ruby: &Ruby, module: RModule) -> Result<(), Error> {
    let batch = crate::class(ruby, module, "Batch")?;
    batch.define_singleton_method("decode", function!(Batch::decode, 3))?;
    batch.define_method("row_count", method!(Batch::row_count, 0))?;
    batch.define_method("schema", method!(Batch::schema, 0))?;
    batch.define_method("ipc", method!(Batch::ipc, 0))?;
    batch.define_method("column", method!(Batch::column, 1))?;
    batch.define_method("cells", method!(Batch::cells, 2))?;
    batch.define_method("states", method!(Batch::states, 1))?;
    batch.define_method("offsets", method!(Batch::offsets, 2))?;
    batch.define_method("fixed", method!(Batch::fixed, 2))?;
    batch.define_method("varlen", method!(Batch::varlen, 2))?;
    let builder = crate::class(ruby, module, "BatchBuilder")?;
    builder.define_singleton_method("new", function!(BatchBuilder::new, 2))?;
    builder.define_method("states", method!(BatchBuilder::states, 2))?;
    builder.define_method("offsets", method!(BatchBuilder::offsets, 3))?;
    builder.define_method("fixed", method!(BatchBuilder::fixed, 3))?;
    builder.define_method("varlen", method!(BatchBuilder::varlen, 4))?;
    builder.define_method("finish", method!(BatchBuilder::finish, 0))?;
    Ok(())
}
