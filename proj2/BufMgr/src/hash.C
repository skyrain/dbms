//--- hash function implementation ---

//--- hash calculation ----------------
//--- calculate the bucket should be --
//--- according to pageId -------------
int BufMgr::hash(PageId pageId)
{
	return (HTA * pageId + HTB) % HTSIZE;
}

//--- delete hash bucket ---------------
void BufMgr::hashRemove(PageId pageId, int frameId)
{
}

//--- insert hash bucket ---------------
void BufMgr::hashPut(PageId pageId, int frameId)
{
}

//--- get frame id ----------------------
//--- return: -1 not page not in pool ---
//--- otherwise return the frameId ------
int BufMgr::hashGetFrameId(PageId pageId)
{
}
