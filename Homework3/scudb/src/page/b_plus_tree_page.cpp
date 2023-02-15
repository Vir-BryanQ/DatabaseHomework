/**
 * b_plus_tree_page.cpp
 */
#include "page/b_plus_tree_page.h"

namespace scudb {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
    bool BPlusTreePage::IsLeafPage() const
    {
        return page_type_ == IndexPageType::LEAF_PAGE;
    }

    bool BPlusTreePage::IsRootPage() const
    {
        // -1
        return parent_page_id_ == INVALID_PAGE_ID;
    }

    void BPlusTreePage::SetPageType(IndexPageType page_type)
    {
        page_type_ = page_type;
    }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
    int BPlusTreePage::GetSize() const
    {
        return size_;
    }

    void BPlusTreePage::SetSize(int size)
    {
        size_ = size;
    }

    void BPlusTreePage::IncreaseSize(int amount)
    {
        size_ += amount;
    }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
    int BPlusTreePage::GetMaxSize() const
    {
        return max_size_;
    }

    void BPlusTreePage::SetMaxSize(int size)
    {
        max_size_ = size;
    }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * With n = 4 in our example B+-tree, each leaf must contain at least 2 values, and at most 3 values.
 */
    int BPlusTreePage::GetMinSize() const
    {
        if(!IsRootPage())
        {// general case
            return (max_size_ ) / 2;

        }
        else
        {
            if(IsLeafPage())
            {// root, leaf, just point to only a page according to its value
                return 1;
            }
            else
            {// root, not leaf, it has at least one leaf page
                return 2;
            }
        }
    }

/*
 * Helper methods to get/set parent page id
 */
    page_id_t BPlusTreePage::GetParentPageId() const
    {
        return parent_page_id_;
    }

    void BPlusTreePage::SetParentPageId(page_id_t parent_page_id)
    {
        parent_page_id_ = parent_page_id;
    }

/*
 * Helper methods to get/set self page id
 */
    page_id_t BPlusTreePage::GetPageId() const
    {
        return page_id_;
    }

    void BPlusTreePage::SetPageId(page_id_t page_id)
    {
        page_id_ = page_id;
    }

/*
 * Helper methods to set lsn
 */
    void BPlusTreePage::SetLSN(lsn_t lsn)
    {
        lsn_ = lsn;
    }

/* for concurrent index */
    bool BPlusTreePage::IsSafe(OpType op)
    {
        int size = GetSize();
        if (op == OpType::INSERT)
        {
            if(size < GetMaxSize())
            {
                return true;
            }
            return false;
        }
        int minSize = GetMinSize() + 1;
        if (op == OpType::DELETE)
        {
            if(IsLeafPage())
            {
                if(size >= minSize)
                {
                    return true;
                }
                return false;
            }
            else
            {
                if(size > minSize)
                {
                    return true;
                }
                return false;
            }
        }
        assert(false);
    }

} // namespace scudb
