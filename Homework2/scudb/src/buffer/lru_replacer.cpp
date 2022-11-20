/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

    template <typename T> LRUReplacer<T>::LRUReplacer() {}

    template <typename T> LRUReplacer<T>::~LRUReplacer()
    {
        //though use smart pointer, there's still may be memory leak
        while (head)
        {
            shared_ptr<DLinkedNode> tmp = head->next;
            head->next = nullptr;
            head = tmp;
        }

        while (tail)
        {
            shared_ptr<DLinkedNode> tmp = tail->pre;
            tail->pre = nullptr;
            tail = tmp;
        }
    }

/*
 * Insert node at the head of LRU
 */

    template <typename T> void LRUReplacer<T>::insertAtHead(shared_ptr<DLinkedNode> node)
    {
        if (node == nullptr) {
            return;
        }
        node->pre = nullptr;
        node->next = head;

        if (head != nullptr) {
            head->pre = node;
        }
        head = node;

        if (tail == nullptr) {
            tail = node;
        }

        index[node->value] = node;
        size++;
    }

/*
 * Insert value into LRU
 */
    template <typename T> void LRUReplacer<T>::Insert(const T &value)
    {
        lock_guard<mutex> guard(latch);
        erase(value);
        auto newNode = make_shared<DLinkedNode>(value);
        insertAtHead(newNode);
    }

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
    template <typename T> bool LRUReplacer<T>::Victim(T &value)
    {
        lock_guard<std::mutex> guard(latch);

        if (size == 0)
        {// LRU is empty
            return false;
        }
        else
        {
            if (head == tail)
            {// there is only one value in LRU
                value = head->value;
                head = nullptr;
                tail = nullptr;
            }
            else
            {
                value = tail->value;
                auto discard = tail;
                discard->pre->next = nullptr;
                tail = discard->pre;
                discard->pre = nullptr;
            }
            index.erase(value);
            size--;
            return true;
        }
    }

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
    template <typename T> bool LRUReplacer<T>::Erase(const T &value)
    {
        lock_guard<std::mutex> guard(latch);
        return erase(value);
    }

    template <typename T> size_t LRUReplacer<T>::Size()
    {
        lock_guard<std::mutex> guard(latch);
        return size;
    }

    template <typename T> bool LRUReplacer<T>::erase(const T &value)
    {
        auto iter = index.find(value);
        if (iter == index.end())
        {
            return false;
        }

        auto ptr = iter->second;
        if (ptr == head && ptr == tail)
        {// there is only one value
            head = nullptr;
            tail = nullptr;
        }
        else if (ptr == head)
        {
            ptr->next->pre = nullptr;
            head = ptr->next;
        }
        else if (ptr == tail)
        {
            ptr->pre->next = nullptr;
            tail = ptr->pre;
        }
        else
        {
            ptr->pre->next = ptr->next;
            ptr->next->pre = ptr->pre;
        }
        ptr->pre = nullptr;
        ptr->next = nullptr;

        index.erase(value);
        size--;
        return true;
    }

    template class LRUReplacer<Page *>;
// test only
    template class LRUReplacer<int>;

} // namespace scudb