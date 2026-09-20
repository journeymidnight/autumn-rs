//! Native LanceDB integration: no FUSE, no S3 gateway.
use std::sync::Arc;

use anyhow::Result;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, types::Float32Type};
use arrow_schema::{DataType, Field, Schema};
use autumn_object_store::AutumnObjectStore;
use futures::TryStreamExt;
use lance::{
    dataset::{ReadParams, WriteParams},
    io::ObjectStoreParams,
};
use lance_table::io::commit::ConditionalPutCommitHandler;
use lancedb::{
    query::{ExecutableQuery, QueryBase},
    table::WriteOptions,
};
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
    let store = Arc::new(AutumnObjectStore::connect(&manager, &scope).await?);
    anyhow::ensure!(
        store.list(None).try_next().await?.is_none(),
        "demo requires an empty dedicated object scope"
    );
    let uri = "memory:///autumn-demo";
    let table_url = url::Url::parse("memory:///autumn-demo/vectors.lance")?;
    let params = ObjectStoreParams {
        object_store: Some((store.clone(), table_url.clone())),
        ..Default::default()
    };
    let write = WriteOptions {
        lance_write_params: Some(WriteParams {
            store_params: Some(params.clone()),
            commit_handler: Some(Arc::new(ConditionalPutCommitHandler)),
            ..Default::default()
        }),
    };
    let db = lancedb::connect(uri).execute().await?;
    let table = db
        .create_table("vectors", batch(0, 100)?)
        .write_options(write.clone())
        .execute()
        .await?;
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

    // Reopen with a separate SDK worker and race independent Lance writers.
    let second = Arc::new(AutumnObjectStore::connect(&manager, &scope).await?);
    let read = ReadParams {
        store_options: Some(ObjectStoreParams {
            object_store: Some((second.clone(), table_url)),
            ..Default::default()
        }),
        commit_handler: Some(Arc::new(ConditionalPutCommitHandler)),
        ..Default::default()
    };
    let reopened = db
        .open_table("vectors")
        .lance_read_params(read)
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
    drop(second);
    println!(
        "vacuum reclaimed {} chunks",
        store.vacuum_quiescent().await?
    );
    Ok(())
}
