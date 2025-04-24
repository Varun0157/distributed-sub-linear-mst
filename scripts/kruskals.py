import sys


class DisjointSet:
    def __init__(self, n):
        self.parent = list(range(n + 1))  # Adjusting for 1-based index
        self.size = [1] * (n + 1)  # Track the size of each set

    def find(self, v):
        if self.parent[v] != v:
            self.parent[v] = self.find(self.parent[v])  # Path compression
        return self.parent[v]

    def union(self, u, v):
        root_u = self.find(u)
        root_v = self.find(v)

        if root_u != root_v:
            if self.size[root_u] > self.size[root_v]:
                self.parent[root_v] = root_u
                self.size[root_u] += self.size[root_v]
            else:
                self.parent[root_u] = root_v
                self.size[root_v] += self.size[root_u]


def kruskal(edges, num_nodes):
    edges.sort(key=lambda x: (x[2], x[0], x[1]))  # Sort edges by weight
    ds = DisjointSet(num_nodes)
    mst = []

    for u, v, w in edges:
        if ds.find(u) != ds.find(v):
            ds.union(u, v)
            mst.append((u, v, w))

    return mst


def read_graph_from_file(filename):
    edges = []
    nodes = set()

    with open(filename, "r") as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) != 3:
                continue
            u, v, w = int(parts[0]), int(parts[1]), int(parts[2])
            edges.append((u, v, w))
            nodes.update([u, v])

    return edges, max(nodes)  # Adjust for 1-based index


def main():
    if len(sys.argv) != 2:
        print("Usage: python kruskal.py <filename>")
        sys.exit(1)

    filename = sys.argv[1]
    edges, num_nodes = read_graph_from_file(filename)
    mst = kruskal(edges, num_nodes)
    mst = sorted(mst, key=lambda x: (x[2], x[0], x[1]))

    verts = []
    weight = 0
    for u, v, w in mst:
        for node in (u, v):
            if node in verts:
                continue
            verts.append(node)
        weight += w

    print(f"edges: {len(mst)}")
    print(f"verts: {len(verts)}")
    print(f"weight: {weight}")


if __name__ == "__main__":
    main()
