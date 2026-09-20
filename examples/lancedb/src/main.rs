//! Native LanceDB integration: no FUSE, no S3 gateway.
//!
//! Just connects. The fork registers the `autumn://` provider on the sessions
//! it creates, so there is no registry to assemble here and no store to inject
//! — which is the point: this is what any LanceDB caller writes, in Rust or in
//! Python, and the demo is only worth something if it writes the same thing.
use std::sync::Arc;

use anyhow::Result;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, types::Float32Type};
use arrow_schema::{DataType, Field, Schema};
use autumn_object_store::AutumnObjectStore;
use futures::TryStreamExt;
use lance::dataset::ReadParams;
use lancedb::query::{ExecutableQuery, QueryBase};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};

fn batch(first: i32, count: i32) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            true,
        ),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(first..first + count)),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    (first..first + count)
                        .map(|i| Some(vec![Some(i as f32), Some(1.0), Some(2.0), Some(3.0)])),
                    4,
                ),
            ),
        ],
    )?)
}

#[tokio::main]
async fn main() -> Result<()> {
    let manager = std::env::var("AUTUMN_MANAGER")?;
    let scope = std::env::var("AUTUMN_OBJECT_SCOPE")?;
    // Kept for the assertions at the end: the demo checks what actually landed
    // in Autumn, which needs a store of its own, independent of the one lance
    // builds through the provider.
    let store = Arc::new(AutumnObjectStore::connect(&manager, &scope).await?);
    anyhow::ensure!(
        store.list(None).try_next().await?.is_none(),
        "demo requires an empty dedicated object scope"
    );

    let uri = format!("autumn://{manager}/autumn-demo");
    // No commit handler is named here on purpose. Upstream lance hands an
    // unknown scheme UnsafeCommitHandler; the fork selects ConditionalPut for
    // autumn://, and this demo is what proves it, so spelling it out would
    // mask the very thing under test.
    let db = lancedb::connect(&uri)
        .storage_option(lancedb::AUTUMN_SCOPE_OPTION, &scope)
        .execute()
        .await?;
    let table = db.create_table("vectors", batch(0, 100)?).execute().await?;
    println!("created 100 rows");
    table.add(batch(100, 100)?).execute().await?;
    println!("appended 100 rows");
    assert_eq!(table.count_rows(None).await?, 200);
    let result = table
        .vector_search(&[42.0f32, 1.0, 2.0, 3.0])?
        .limit(1)
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let ids = result[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 42);
    table.delete("id < 10").await?;
    assert_eq!(table.count_rows(None).await?, 190);

    // Reopen through a SECOND connection, so the two writers below reach the
    // table through independent sessions and stores rather than sharing one.
    let second_db = lancedb::connect(&uri)
        .storage_option(lancedb::AUTUMN_SCOPE_OPTION, &scope)
        .execute()
        .await?;
    let reopened = second_db
        .open_table("vectors")
        .lance_read_params(ReadParams::default())
        .execute()
        .await?;
    assert_eq!(reopened.count_rows(None).await?, 190);
    let left = table.add(batch(200, 10)?).execute();
    let right = reopened.add(batch(300, 10)?).execute();
    let (a, b) = tokio::join!(left, right);
    a?;
    b?;
    table.checkout_latest().await?;
    assert_eq!(table.count_rows(None).await?, 210);

    let objects: Vec<_> = store
        .list(Some(&Path::from("autumn-demo/vectors.lance")))
        .try_collect()
        .await?;
    assert!(
        objects
            .iter()
            .any(|m| m.location.as_ref().contains("_versions/"))
    );
    println!(
        "native LanceDB: create, append, vector search, delete, reopen, concurrent append passed; {} objects",
        objects.len()
    );
    // The demo owns its explicitly selected scope; remove only its table files.
    for meta in objects {
        store.delete(&meta.location).await?;
    }
    drop(table);
    drop(reopened);
    drop(db);
    drop(second_db);
    println!(
        "vacuum reclaimed {} chunks",
        store.vacuum_quiescent().await?
    );
    Ok(())
}
