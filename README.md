# bptree-wal

B+tree en disco, por páginas de 4 KiB, con un write-ahead log de *redo* que sobrevive a
`kill -9` del proceso escritor. Rust, sin `unsafe`, una sola dependencia (`crc32fast`).

*English below.*

## Español

### Modelo de durabilidad

- Una transacción (`Db::write`) agrega al WAL la imagen completa de cada página que cambió más
  la página meta, cierra con un registro *commit* con CRC32 y hace `fsync` del WAL **antes**
  de devolver. Cada registro lleva magic, tipo, id de transacción, página, largo y CRC.
- El archivo de datos sólo lo toca el *checkpoint* (no-steal): escribe las páginas cambiadas
  desde el anterior, `fsync` del archivo de datos, y recién después trunca el WAL.
- Al abrir se re-aplican al archivo de datos las transacciones con *commit* íntegro; la cola
  rota o dañada se descarta; luego `fsync` y truncado. La re-aplicación es idempotente.

Resultado: después de un crash en cualquier instante, el árbol reabre en el estado de la última
transacción cuyo *commit* llegó entero al disco.

### API

```rust
let mut db = bptree_wal::Db::open("dir")?;
db.put(b"k", b"v")?;                       // una transacción
db.write(|tx| { tx.put(b"a", b"1")?; tx.delete(b"b")?; Ok(()) })?; // atómica
db.get(b"k")?;  db.scan(Some(b"a"), Some(b"m"))?;  db.verify()?;
db.close()?;                               // checkpoint; soltar el Db sin close = crash
```

Claves de hasta 128 bytes y valores de hasta 1024. Si el closure de `write` falla, no se
aplica nada ni se consume id. CLI: `bptree-wal put|get|del|scan|writer|check|bench`.

### Tests (17 + proptest)

- **Unitarios (8):** codificación de páginas y meta con CRC, cola rota y byte dañado en el WAL,
  punto de corte de splits, ruteo de claves iguales.
- **Modelo (5):** secuencias aleatorias de put/delete/lotes/checkpoint/**crash** (soltar sin
  cerrar y reabrir) contra `BTreeMap`, 48 casos de hasta 250 operaciones, con claves y valores
  de tamaño máximo; rangos de `scan`; 40 000 claves ascendentes y descendentes; límites y
  transacción fallida; archivo ajeno rechazado. `verify()` revisa orden, cotas del padre,
  profundidad uniforme de hojas, cadena de hojas completa y ninguna página alcanzable dos veces.
- **Recuperación (4):**
  - WAL truncado en cada borde de transacción, ±1 byte y 200 cortes intermedios: el árbol
    reabre **exactamente** en la última transacción completa;
  - un byte invertido al principio, medio y final de cada transacción: el replay se detiene
    justo antes;
  - replay interrumpido antes del truncado y repetido: mismo árbol;
  - **`SIGKILL` real:** un proceso hijo corre una carga determinista e imprime `ack N` sólo
    después del `fsync`; el test lo mata en 12 momentos distintos (con checkpoints de 16 KiB,
    64 KiB y 1 MiB, retomando sobre el mismo directorio) y exige que el estado recuperado sea
    `N` o `N+1` y que coincida clave por clave con el modelo.
- Mutación de control: aceptar páginas sin *commit* en el replay hace fallar 2 de los 4 tests
  de recuperación.

CI: fmt, clippy, `cargo test` en x86-64 y AArch64, 5 rondas extra del test de `SIGKILL` en
release y un bench informativo.

### Rendimiento medido

`bptree-wal bench`, release, claves de 16 bytes dispersas, valores de 100 bytes, ext4 sobre
disco virtio de una VM de 8 vCPU:

| operación | resultado |
|---|---:|
| put, 1 clave por transacción, `fsync` cada una | 6 668 txn/s |
| put, 1000 claves por transacción (200 k claves, con checkpoint final) | 40 540 claves/s |
| get aleatorio, caché caliente | 811 839 claves/s |
| scan completo | 4,8 M claves/s |

El WAL de imágenes de página tiene **mucha amplificación de escritura**: esas 200 k claves
(~23 MB de datos) escribieron 800 MB de WAL, porque cada lote de claves dispersas toca casi
todas las hojas. Es el costo de la simplicidad del redo por página completa.

### Límites

- Un solo escritor por directorio, sin locking entre procesos.
- `delete` no fusiona hojas y no hay lista de páginas libres: el archivo no se achica.
- Todas las páginas cambiadas desde el último checkpoint viven en memoria (acotado por
  `checkpoint_bytes`, 8 MiB por defecto); las leídas quedan en caché sin desalojo.
- Sin WAL de deltas ni group commit.

## English

Page-based on-disk B+tree (4 KiB pages) with a redo write-ahead log that survives `kill -9`
of the writer. Safe Rust, one dependency.

**Durability:** a transaction appends full images of its changed pages plus the meta page and
a CRC-protected commit record to the WAL and fsyncs it before returning. The data file is only
written by checkpoints (write changed pages, fsync, then truncate the WAL). Opening replays
every intact committed transaction, discards a torn or damaged tail, and is idempotent. After
a crash at any instant the tree reopens as of the last transaction whose commit reached disk.

**Tests:** 8 unit; 5 model-based (random put/delete/batch/checkpoint/**crash-and-reopen**
sequences against `BTreeMap`, range scans, 40 k ascending and descending keys, limits, failed
transactions, foreign files) with a full structural `verify()`; 4 recovery tests: WAL cut at
every commit boundary, ±1 byte and 200 interior points recovers exactly the complete prefix;
a flipped byte stops replay right before that transaction; interrupted replay is idempotent;
and a **real `SIGKILL`** test that kills a child writer at 12 points (16 KiB / 64 KiB / 1 MiB
checkpoints, resuming in the same directory) and requires the recovered state to be the last
acknowledged transaction or the next one, matching the model key by key. A control mutation
(replaying uncommitted pages) makes 2 of the 4 recovery tests fail. CI runs fmt, clippy and
the tests on x86-64 and AArch64, plus 5 extra SIGKILL rounds in release.

**Measured** (release, scattered 16-byte keys, 100-byte values, ext4 on a virtio disk, 8-vCPU
VM): 6 668 fsynced single-key txn/s; 40 540 keys/s in 1000-key transactions; 812 k random
gets/s warm; 4.8 M keys/s full scan. The page-image WAL has heavy write amplification: 200 k
keys (~23 MB) wrote 800 MB of WAL.

**Limits:** single writer per directory; no leaf merging or free list; pages changed since the
last checkpoint are held in memory (bounded by `checkpoint_bytes`); no delta logging or group
commit.

## License

Apache-2.0.
