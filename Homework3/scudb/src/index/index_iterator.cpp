/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace scudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int index, BufferPoolManager *bufferPoolManager)
    : index_(index),leaf_(leaf), bufferPoolManager_(bufferPoolManager){}

    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE::~IndexIterator() {
        if (leaf_ != nullptr)
        {
            UnlockAndUnPin();
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    bool INDEXITERATOR_TYPE::isEnd()
    {
        return leaf_ == nullptr;
    }


    template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
    template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
    template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
    template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
    template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace scudb
