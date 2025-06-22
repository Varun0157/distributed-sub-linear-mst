An implementation of the calculation of a Minimum Spanning Tree of a given graph in a distributed setting, assuming sub-linear memory per node, as defined [by Mohsen Ghaffari](https://people.csail.mit.edu/ghaffari/MPA19/Notes/MPA.pdf).

It sports red-blue randomness and a tree-like structure of distributed nodes with sub-linear memory, with mutexes to control the flow of edges up and down. It is inspired by [an assignment document from the University of Freiburg](./docs/solution_12.pdf).

### Run

Set up the project:

```bash
cd src
go mod tidy
```

Create a random large graph, calculate the MST using sequential methods, and then compare it with the distributed implementation:

```bash
./test.sh
```

### Credits

[Prof. Kishore Kothapalli](https://scholar.google.com/citations?user=fKTjFPIAAAAJ&hl=en) for his guidance and knowledge of the above algorithms.
