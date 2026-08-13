# epoch-sync-proof-size

Measures how big an `EpochSyncProof` response is, and how much it grows per epoch.

The tool opens a raw peer connection to a running node, sends `EpochSyncRequest`, and
measures the `EpochSyncResponse` the node sends back. This is the same message a node
downloads when it epoch syncs, so the compressed size is what a new node pays today.

## Getting a peer

Any node with a stored proof works. To list peers of a public RPC node:

```
curl -s -X POST https://rpc.mainnet.near.org -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"1","method":"network_info","params":[]}' \
  | jq -r '.result.active_peers[] | "\(.id)@\(.addr)"'
```

## Usage

```
neard epoch-sync-proof-size fetch \
  --chain-id mainnet \
  --peer ed25519:8hwx9p1Gjr9ZBBPXwi9YLMq9FrtSmB8hzXi3nbFovEn8@52.193.158.166:24567 \
  --save-proof mainnet-proof.bin \
  --csv mainnet-epochs.csv
```

The proof file holds the compressed bytes exactly as they came over the wire, so the
report can be reproduced without the network:

```
neard epoch-sync-proof-size analyze mainnet-proof.bin --csv mainnet-epochs.csv
```

The handshake is retried once with the protocol version the peer reports, so a build that
runs ahead of the network still connects. Use `--protocol-version` to pick one yourself.

## What the report shows

* Compressed size (the bytes on the wire) and uncompressed borsh size.
* The split between `all_epochs`, `last_epoch`, and `current_epoch`.
* The newest epoch entry, split into block producers, endorsements, and block header.
* Growth per epoch. `all_epochs` gains one entry per epoch and nothing is ever dropped,
  so this is the growth rate of the whole message.
  * Uncompressed: mean size of the newest `--marginal-epochs` entries.
  * Compressed: the full proof is compressed again with its newest `--marginal-epochs`
    entries removed, and the difference is divided by that count.
* A projection, using the mean epoch duration taken from the block header timestamps
  in the proof.

The `--csv` file has one row per epoch entry (height, timestamp, block producer count,
endorsement count, and byte sizes), for plotting the growth over the chain's history.

## Measurements on 2026-08-13

| | mainnet | testnet |
| --- | --- | --- |
| epochs in the proof | 4658 | 5122 |
| on the wire (compressed) | 46.83 MiB | 18.85 MiB |
| uncompressed | 71.86 MiB | 30.67 MiB |
| block producers | 100 | 20 |
| growth, uncompressed | 18.51 KiB/epoch | 4.44 KiB/epoch |
| growth, compressed | 11.48 KiB/epoch | 2.79 KiB/epoch |
| mean epoch duration | 11.4h | 9.2h |
| growth, compressed | 8.62 MiB/year | 2.60 MiB/year |

`all_epochs` holds 99.5% of the bytes on mainnet, and one entry is added per epoch, so
the message grows for as long as the chain runs. The entry size follows the block
producer count: mainnet entries grew from 6.2 KiB (33 block producers, 2020) to
18.4 KiB (100 block producers, 2026), and testnet entries shrank from 11.6 KiB
(2022) to 4.3 KiB after the validator set was cut to 20.
