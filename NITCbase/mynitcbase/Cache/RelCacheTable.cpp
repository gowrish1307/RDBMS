#include "RelCacheTable.h"

#include <cstring>

RelCacheEntry* RelCacheTable::relCache[MAX_OPEN];

/*
Get the relation catalog entry for the relation with rel-id `relId` from the cache
NOTE: this function expects the caller to allocate memory for `*relCatBuf`
*/
int RelCacheTable::getRelCatEntry(int relId, RelCatEntry* relCatBuf) {
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  // if there's no entry at the rel-id
  if (relCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  // copy the value to the relCatBuf argument
  *relCatBuf = relCache[relId]->relCatEntry;

  return SUCCESS;
}

/* Converts a relation catalog record to RelCatEntry struct
    We get the record as Attribute[] from the BlockBuffer.getRecord() function.
    This function will convert that to a struct RelCatEntry type.
NOTE: this function expects the caller to allocate memory for `*relCatEntry`
*/
void RelCacheTable::recordToRelCatEntry(union Attribute record[RELCAT_NO_ATTRS],RelCatEntry* relCatEntry) {
  strcpy(relCatEntry->relName, record[RELCAT_REL_NAME_INDEX].sVal);
  relCatEntry->numAttrs = (int)record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
  relCatEntry->numRecs = (int)record[RELCAT_NO_RECORDS_INDEX].nVal;
  relCatEntry->firstBlk = (int)record[RELCAT_FIRST_BLOCK_INDEX].nVal;
  relCatEntry->lastBlk = (int)record[RELCAT_LAST_BLOCK_INDEX].nVal;
  relCatEntry->numSlotsPerBlk = (int)record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal;
  int block=relCatEntry->firstBlk;
  int count=0;
  while(block!=-1)
  {
    RecBuffer recBuffer(block);
    HeadInfo header;
    recBuffer.getHeader(&header);
    count++;
    block=header.rblock;
  }
relCatEntry->numBlks=count;
  /* fill the rest of the relCatEntry struct with the values at
      RELCAT_NO_RECORDS_INDEX,
      RELCAT_FIRST_BLOCK_INDEX,
      RELCAT_LAST_BLOCK_INDEX,
      RELCAT_NO_SLOTS_PER_BLOCK_INDEX
  */
}

void RelCacheTable::relCatEntryToRecord(RelCatEntry* relCatEntry,union Attribute record[RELCAT_NO_ATTRS])
{
  strcpy(record[RELCAT_REL_NAME_INDEX].sVal,relCatEntry->relName);
  record[RELCAT_NO_ATTRIBUTES_INDEX].nVal = relCatEntry->numAttrs;
  record[RELCAT_NO_RECORDS_INDEX].nVal = relCatEntry->numRecs;
  record[RELCAT_FIRST_BLOCK_INDEX].nVal = relCatEntry->firstBlk;
  record[RELCAT_LAST_BLOCK_INDEX].nVal = relCatEntry->lastBlk;
  record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = relCatEntry->numSlotsPerBlk;
}


/* will return the searchIndex for the relation corresponding to `relId
NOTE: this function expects the caller to allocate memory for `*searchIndex`
*/
int RelCacheTable::getSearchIndex(int relId, RecId* searchIndex) {
  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  // check if relCache[relId] == nullptr and return E_RELNOTOPEN if true
  if (relCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }


  // copy the searchIndex field of the Relation Cache entry corresponding
  //   to input relId to the searchIndex variable.
  *searchIndex = relCache[relId]->searchIndex;
  return SUCCESS;
}

// sets the searchIndex for the relation corresponding to relId
int RelCacheTable::setSearchIndex(int relId, RecId* searchIndex) {

  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  // check if relCache[relId] == nullptr and return E_RELNOTOPEN if true
  if (relCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  // update the searchIndex value in the relCache for the relId to the searchIndex argument
  relCache[relId]->searchIndex = *searchIndex;

  return SUCCESS;
}

int RelCacheTable::resetSearchIndex(int relId) {
  // use setSearchIndex to set the search index to {-1, -1}
  RecId searchIndex;
  searchIndex.block=-1;
  searchIndex.slot=-1;
  setSearchIndex(relId,&searchIndex);
  return SUCCESS;
}

int RelCacheTable::setRelCatEntry(int relId, RelCatEntry *relCatBuf) {

  if(relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if(relCache[relId] == nullptr) {
    return E_RELNOTOPEN;
  }

  // copy the relCatBuf to the corresponding Relation Catalog entry in
  // the Relation Cache Table.
  relCache[relId]->relCatEntry = *relCatBuf;
  // set the dirty flag of the corresponding Relation Cache entry in
  // the Relation Cache Table.
  relCache[relId]->dirty = true;

  return SUCCESS;
}

