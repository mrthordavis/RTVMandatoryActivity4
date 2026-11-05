# RTVMandatoryActivity4

Repository to solve distributed systems autumn 2026 mandatory activity 4

## How to run

1. Start 4 terminal windows in the repo root (/RTVMandatoryActivity4)
2. Run each node/peer with "go run node.go x", where x is the letters a-d
3. Write request in one of the terminal windows to manually trigger when a server should request.

## Known issues

- Cannot work properly without all 4 nodes being run, as the number of nodes is hardcoded in idMap.txt.
