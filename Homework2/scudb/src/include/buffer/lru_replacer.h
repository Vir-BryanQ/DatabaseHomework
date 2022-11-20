/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include <map>
#include <mutex>
#include <memory>

#include "buffer/replacer.h"
#include "hash/extendible_hash.h"

using namespace std;

namespace scudb {

    template <typename T> class LRUReplacer : public Replacer<T>
    {
    public:
        // do not change public interface
        LRUReplacer();

        ~LRUReplacer();

        void Insert(const T &value);

        bool Victim(T &value);

        bool Erase(const T &value);

        size_t Size();

    private:
        // add your member variables here
        struct DLinkedNode
        {
            DLinkedNode(T v) :value(v) {}
            T value;
            shared_ptr<DLinkedNode> pre;
            shared_ptr<DLinkedNode> next;
        };


        void insertAtHead(shared_ptr<DLinkedNode> node);
        bool erase(const T &value);

        shared_ptr<DLinkedNode> head = nullptr;
        shared_ptr<DLinkedNode> tail = nullptr;
        int size = 0;
        mutex latch;
        map<T, shared_ptr<DLinkedNode>> index;
    };

} // namespace scudb