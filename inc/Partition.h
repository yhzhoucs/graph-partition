#pragma once

#include "CSR.h"
#include <fstream>
#include <cassert>

namespace my {
    template<typename _Node, typename _PIdx>
    class Partition {
    private:
        my::CSR<_Node> &csr;
        _PIdx partition_number;
        _PIdx *partition;
    public:
        Partition()=default;
        Partition(my::CSR<_Node> &_csr);
        ~Partition();

        _PIdx get_partition_number() const { return partition_number; }

        std::ifstream &load_partition(std::ifstream &infile);

        const _PIdx& operator[](size_t n) const {
            assert(n < csr.get_vertex_number());
            return partition[n];
        }
    };
}

template<typename _Node, typename _PIdx>
my::Partition<_Node, _PIdx>::Partition(my::CSR<_Node> &_csr) : csr{_csr} {
    partition = new _PIdx[_csr.get_vertex_number()];
}

template<typename _Node, typename _PIdx>
my::Partition<_Node, _PIdx>::~Partition() {
    if (partition) delete[] partition;
}

template<typename _Node, typename _PIdx>
std::ifstream &my::Partition<_Node, _PIdx>::load_partition(std::ifstream &infile) {
    _PIdx max_n{0};
    int n;
    for (_Node i = 0; i < csr.get_vertex_number(); ++i) {
        infile >> n;
        partition[i] = n;
        max_n = std::max(max_n, static_cast<_PIdx>(n));
    }
    partition_number = max_n + 1;
}
