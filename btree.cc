#include <assert.h>
#include "btree.h"
#include <stdlib.h>
#include <vector>
#include <math.h>
KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}

ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
        	// OK, so we now have the first key that's larger
        	// so we ned to recurse on the ptr immediately previous to 
        	// this one, if it exists
        	rc=b.GetPtr(offset,ptr);
        	if (rc) { return rc; }
	        return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
        	if (op==BTREE_OP_LOOKUP) { 
        	  return b.GetVal(offset,value);
        	} else if (op==BTREE_OP_UPDATE){ 
        	  // BTREE_OP_UPDATE
        	  // WRITE ME
                 ERROR_T setValErr = b.SetVal(offset,value);
                 if(setValErr != ERROR_NOERROR) { return setValErr; }
                 ERROR_T serializeBErr = b.Serialize(buffercache,node);
                 if(serializeBErr != ERROR_NOERROR){ return serializeBErr; }
                 return ERROR_NOERROR;
        	  //return ERROR_UNIMPL;
        	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) { 
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  VALUE_T valueparam = value; //compiler won't accept a const
  SIZE_T newDiskBlock;
  KEY_T newPromotedKey; 
  return InsertHelper(superblock.info.rootnode, key, valueparam, newDiskBlock,newPromotedKey);
  // WRITE ME
  //return ERROR_UNIMPL;
}


ERROR_T BTreeIndex::InsertHelper(const SIZE_T &node, const KEY_T &key, const VALUE_T &value, SIZE_T &newDiskBlock, KEY_T &newPromotedKey)
{
    BTreeNode b;
    ERROR_T rc;
    SIZE_T offset;
    KEY_T testkey;
    SIZE_T ptr;
    KEY_T lastKey; // For use in the special case when the inserted key is the largest one in the leaf node

    rc= b.Unserialize(buffercache,node);

    if (rc!=ERROR_NOERROR) { 
      return rc;
    }
    
    switch (b.info.nodetype) { 
    case BTREE_ROOT_NODE:

      cerr << "root keys: ";
      cerr <<  b.info.numkeys; 
      cerr << "\n";

      if(b.info.numkeys==0){ //first insert
        //Allocate new leaf nodes
        SIZE_T leftLeafBlock;
        SIZE_T rightLeafBlock;
        BTreeNode leftLeaf(BTREE_LEAF_NODE,superblock.info.keysize,superblock.info.valuesize,superblock.info.blocksize);
        BTreeNode rightLeaf(BTREE_LEAF_NODE,superblock.info.keysize,superblock.info.valuesize,superblock.info.blocksize);
        rc = AllocateNode(leftLeafBlock);
        if (rc!=ERROR_NOERROR) { return rc; }
        rc = AllocateNode(rightLeafBlock);
        if (rc!=ERROR_NOERROR) { return rc; }
        
        //Set up root node
        b.info.numkeys++;
        b.SetKey(0,key);
        b.SetPtr(0,leftLeafBlock);
        b.SetPtr(1,rightLeafBlock);
        

        //Set up leaf nodes
        leftLeaf.info.numkeys++;
        leftLeaf.SetKey(0,key);
        leftLeaf.SetVal(0,value);
        
        //Save nodes
        rc = b.Serialize(buffercache,node);
        if (rc!=ERROR_NOERROR) { return rc; }
        rc = leftLeaf.Serialize(buffercache,leftLeafBlock);
        if (rc!=ERROR_NOERROR) { return rc; }
        rc = rightLeaf.Serialize(buffercache,rightLeafBlock);
        if (rc!=ERROR_NOERROR) { return rc; }
        
        return ERROR_NOERROR;
        break;
      }else{
          //do the same as you'd do for an interior node.
      }
      

    case BTREE_INTERIOR_NODE: 

     cerr << "int keys: ";
     cerr <<  b.info.numkeys; 
     cerr << "\n";
     cerr << "int slots: ";
     cerr << b.info.GetNumSlotsAsInterior();
     cerr << "\n";
    
    // Scan through key/ptr pairs and recurse if possible

     for (offset=0;offset<b.info.numkeys;offset++) { 
        rc=b.GetKey(offset,testkey);
        if (rc) {  return rc; }
        if (key<testkey || key==testkey) {
            if(key == testkey){
              return ERROR_UNIQUE_KEY; // we cannot insert non unique keys
            }
            // OK, so we now have the first key that's larger
            // so we ned to recurse on the ptr immediately previous to 
            // this one, if it exists
            rc=b.GetPtr(offset,ptr);
            if (rc) { return rc; }
            ERROR_T InsertErr;
            InsertErr = InsertHelper(ptr,key,value,newDiskBlock,newPromotedKey);

            if(InsertErr == ERROR_SPLIT_BLOCK){ // the node below split, we have to promote a key

                //shift the last pointer
                SIZE_T lastPointer;
                SIZE_T originalNumKeys = b.info.numkeys;
                b.info.numkeys++;
                rc = b.GetPtr(originalNumKeys ,lastPointer);
                if (rc!=ERROR_NOERROR) { return rc; }
                rc = b.SetPtr(originalNumKeys+1, lastPointer);
                if (rc!=ERROR_NOERROR) { return rc; }

                //shift all the other keys over.
                for(SIZE_T i =originalNumKeys-1; i >= offset; i--){ // move the others over.
                  KEY_T movedkey;
                  SIZE_T movedpointer;

                  rc = b.GetKey(i,movedkey);
                  if (rc!=ERROR_NOERROR) { return rc; }
                  rc = b.GetPtr(i,movedpointer);
                  if (rc!=ERROR_NOERROR) { return rc; }

                  rc = b.SetKey(i+1,movedkey);
                  if (rc!=ERROR_NOERROR) { return rc; }
                  rc = b.SetPtr(i+1,movedpointer);
                  if (rc!=ERROR_NOERROR) { return rc; }

                  if(i==0){
                    break; // we need this because i is unsigned, i-- will cause it to loop back to positive
                  }
                }

                //set the new promoted key
                rc = b.SetKey(offset, newPromotedKey);
                if (rc!=ERROR_NOERROR) { return rc; }
                rc = b.SetPtr(offset, newDiskBlock);
                if (rc!=ERROR_NOERROR) { return rc; }

                cerr << newDiskBlock << node;
            }else{
                return InsertErr; // if we didn't have to promote a key, we can directly return
            }
            
            splitInteriorNode:

            if((b.info.numkeys - b.info.GetNumSlotsAsInterior()) <= 1){ //if we need to split ourselves
                cerr << "here";

                BTreeNode newNode(BTREE_INTERIOR_NODE,superblock.info.keysize,superblock.info.valuesize,superblock.info.blocksize);

                rc = AllocateNode(newDiskBlock); // want to return this pointer
                if (rc!=ERROR_NOERROR) { return rc; }

                //copy elements to new node: new node would have the bigger half, b would have the smaller half. 
                //b would get saved in the new Disk Block, new node would replace b's disk block

                SIZE_T j = 0;
                //get the last pointer
                SIZE_T lastPointer;
                rc = b.GetPtr(b.info.numkeys,lastPointer);
                if (rc!=ERROR_NOERROR) { return rc; }

                SIZE_T originalNumKeysB = b.info.numkeys;


                //copy half the elements to the new node
                for(SIZE_T i = (b.info.numkeys/2) + 1; i<originalNumKeysB; i++){
                  newNode.info.numkeys++;
                  KEY_T movedkey;
                  SIZE_T movedpointer;

                  //Get key from b, set key in new node
                  rc = b.GetKey(i,movedkey);
                  if (rc!=ERROR_NOERROR) { return rc; }
                  rc = b.GetPtr(i,movedpointer);
                  if (rc!=ERROR_NOERROR) { return rc; }
                  rc = newNode.SetKey(j,movedkey);
                  if (rc!=ERROR_NOERROR) { return rc; }
                  rc = newNode.SetPtr(j,movedpointer);
                  if (rc!=ERROR_NOERROR) { return rc; }
                  j++;
                }

                b.info.numkeys -= j; //do I just decrement or do I need to clear the key val somehow?

                //shift the last pointer
                rc = newNode.SetPtr(j, lastPointer);
                if (rc!=ERROR_NOERROR) { return rc; }
                rc = b.GetKey(b.info.numkeys-1,newPromotedKey); // get the key to be promoted
                if (rc!=ERROR_NOERROR) { return rc; }
                b.info.numkeys --;//promoted key leaves the interior node

                //save the new nodes
                rc = newNode.Serialize(buffercache,node);
                if (rc!=ERROR_NOERROR) { return rc; }
                rc = b.Serialize(buffercache, newDiskBlock);
                if (rc!=ERROR_NOERROR) { return rc; }

                if(b.info.nodetype == BTREE_ROOT_NODE){ //special case : if it is the root that splits
                    //change this to an interior node
                    b.info.nodetype = BTREE_INTERIOR_NODE;
                    BTreeNode newRoot(BTREE_ROOT_NODE,superblock.info.keysize,superblock.info.valuesize,superblock.info.blocksize);
                    SIZE_T newRootBlock;
                    
                    newRoot.info.numkeys++;
                    rc = newRoot.SetKey(0,newPromotedKey);
                    if (rc!=ERROR_NOERROR) { return rc; }
                    rc = newRoot.SetPtr(0,newDiskBlock);
                    if (rc!=ERROR_NOERROR) { return rc; }
                    rc = newRoot.SetPtr(1, node);
                    if (rc!=ERROR_NOERROR) { return rc; }
                    //save new root

                    rc = AllocateNode(newRootBlock); // Do I need this??
                    if (rc!=ERROR_NOERROR) { return rc; }
                    rc = b.Serialize(buffercache, newDiskBlock); //saving the old root as an interior node
                    if (rc!=ERROR_NOERROR) { return rc; }
                    
                    rc = newRoot.Serialize(buffercache, newRootBlock); // save new new root node
                    if (rc!=ERROR_NOERROR) { return rc; }
                    
                    superblock.info.rootnode = newRootBlock;
                    cerr << superblock << endl << "new root block" << newRootBlock <<endl << newDiskBlock << node;
                    
                    //save superblock
                    rc = superblock.Serialize(buffercache,superblock_index);
                    if (rc!=ERROR_NOERROR) { return rc; }

                    return ERROR_NOERROR;
                }else{ // if not the root
                    return ERROR_SPLIT_BLOCK;
                }
                
          }else{
            //no need to split
              return b.Serialize(buffercache,node); // can save directly
          }
        }
      }
      // if we got here, we need to go to the next pointer, if it exists (this is the last pointer)
      if (b.info.numkeys>0) { 

        rc=b.GetPtr(b.info.numkeys,ptr);
        if (rc) { return rc; }

        ERROR_T InsertErr = InsertHelper(ptr,key,value,newDiskBlock,newPromotedKey);

        if(InsertErr == ERROR_SPLIT_BLOCK){
            //move last pointer over
            SIZE_T lastPointer;
            SIZE_T originalNumKeys = b.info.numkeys;
            b.info.numkeys++;
            rc = b.GetPtr(originalNumKeys,lastPointer);
            if (rc!=ERROR_NOERROR) { return rc; }
            rc = b.SetPtr(originalNumKeys + 1, lastPointer);
            if (rc!=ERROR_NOERROR) { return rc; }

            //insert new pointer at the original spot
            rc = b.SetKey(originalNumKeys, newPromotedKey);
            if (rc!=ERROR_NOERROR) { return rc; }
            rc = b.SetPtr(originalNumKeys, newDiskBlock);
            if (rc!=ERROR_NOERROR) { return rc; }
            
            //need to check if you need to split again
            goto splitInteriorNode;
        }else if (InsertErr == ERROR_UNIQUE_KEY){
          return InsertErr;
        }else{
          return b.Serialize(buffercache,node);//save directly.
        }
      } else {
        // There are no keys at all on this node, so nowhere to go
        return ERROR_NONEXISTENT;
      }
      break;

    case BTREE_LEAF_NODE:
     cerr << "leaf keys: ";
     cerr <<  b.info.numkeys; 
     cerr << "\n";

      // Scan through keys looking for matching value 
      // NOTE! Need to handle the case when the value is inserted at the end on the right leaf (the biggest element)

      //Special cases:
      //If the leaf node does not have any keys, add first key
        if(b.info.numkeys==0){
          b.info.numkeys++;
          rc = b.SetKey(0,key);
          if (rc!=ERROR_NOERROR) { return rc; }
          rc = b.SetVal(0,value);
          if (rc!=ERROR_NOERROR) { return rc; }
          return b.Serialize(buffercache,node);
        }
      //If this is the largest key, just put it at the end.

        b.GetKey(b.info.numkeys-1,lastKey); ///MADE this CHANGE

        if(lastKey<key){
          b.info.numkeys++;
          rc = b.SetKey(b.info.numkeys -1,key);
          if (rc!=ERROR_NOERROR) { return rc; }
          rc = b.SetVal(b.info.numkeys -1,value);
          if (rc!=ERROR_NOERROR) { return rc; }
        } else{ // iterate through each key, find the spot
            
            for (offset=0;offset<b.info.numkeys;offset++) { 
              rc=b.GetKey(offset,testkey);
              if (rc) {  return rc; }
              if (key < testkey || key == testkey) { //we want to insert it here, move everything over first
                if(key == testkey){
                    return ERROR_UNIQUE_KEY; // we cannot insert non unique keys
                }

                SIZE_T originalNumKeys = b.info.numkeys;
                b.info.numkeys ++;

                for(SIZE_T i = originalNumKeys-1; i >= offset; i--){ // move the others over.
                  KeyValuePair kvp;
                  rc = b.GetKeyVal(i,kvp);
                  if (rc!=ERROR_NOERROR) { return rc; }
                  rc = b.SetKeyVal(i+1,kvp);
                  if (rc!=ERROR_NOERROR) { return rc; }
                  if(i==0){ break; }// we need this because i is unsigned, i-- will cause it to loop back to positive
                }

                rc = b.SetKeyVal(offset,KeyValuePair(key,value));
                if (rc!=ERROR_NOERROR) { return rc; }

                break;
              }
            }
      }
      
      if((b.info.numkeys-b.info.GetNumSlotsAsLeaf()) <=1){ //if we need to split, setting it to 130 so it'll split early
          

          BTreeNode newNode(BTREE_LEAF_NODE,superblock.info.keysize,superblock.info.valuesize,superblock.info.blocksize);
          
          rc = AllocateNode(newDiskBlock);//want to return this pointer.
          if (rc!=ERROR_NOERROR) { return rc; }
           
          //copy half the elements to a new node
          KeyValuePair kvp;
          SIZE_T j = 0;

          SIZE_T originalNumKeysB = b.info.numkeys;
          for(SIZE_T i = b.info.numkeys/2; i<originalNumKeysB; i++){ //newNode has the bigger half, b has the smaller half. We want to save newNode in the original disk block, and b in the new disk block
            rc = b.GetKeyVal(i,kvp);
            if (rc!=ERROR_NOERROR) { return rc; }
            newNode.info.numkeys++;
            rc = newNode.SetKeyVal(j,kvp);
            if (rc!=ERROR_NOERROR) { return rc; }
            j++;
          }
          b.info.numkeys -= j; //do I just decrement or do I need to clear the key val somehow?

          cerr << newDiskBlock << node;

          //set the new promoted key
          rc = b.GetKey(b.info.numkeys-1, newPromotedKey);
          if (rc!=ERROR_NOERROR) { return rc; }

          //save the nodes
          rc = newNode.Serialize(buffercache,node);
          if (rc!=ERROR_NOERROR) { return rc; }
          rc = b.Serialize(buffercache,newDiskBlock);
          if (rc!=ERROR_NOERROR) { return rc; }

          return ERROR_SPLIT_BLOCK;
      }else{
          return b.Serialize(buffercache,node); // can save directly
      }
      

      break;

    default:
      // We can't be looking at anything other than a root, internal, or leaf
      return ERROR_INSANE;
      break;
    }  
    return ERROR_INSANE;
}




ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  VALUE_T valueparam = value; //checking to see if the comiler will accept it if it's not a const.
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, valueparam);

}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
    return DeleteHelper(superblock.info.rootnode, key);
}

  // This is optional extra credit 
  // delete leaf node

  // delete interior node
ERROR_T BTreeIndex::DeleteHelper(const SIZE_T &node, const KEY_T &key)    
{    
    BTreeNode b;
    ERROR_T rc;
    SIZE_T offset;
    KEY_T testkey;
    SIZE_T ptr;
    KEY_T lastKey; // For use in the special case when the inserted key is the largest one in the leaf node

    rc= b.Unserialize(buffercache,node);

    if (rc!=ERROR_NOERROR) { 
      return rc;
    }

    switch (b.info.nodetype) { 
    case BTREE_ROOT_NODE:
        break;
    case BTREE_INTERIOR_NODE:
        for (offset=0;offset<b.info.numkeys;offset++) { 
            rc=b.GetKey(offset,testkey);
            if (rc) {  return rc; }
            if (key<testkey ) {
            rc=b.GetPtr(offset,ptr);
            if (rc) { return rc; }
            return  DeleteHelper(ptr,key);
            }
        }
        return ERROR_NONEXISTENT;
        break;
    case BTREE_LEAF_NODE:
      for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
    
        if(b.info.numkeys > 1){
            b.info.numkeys--;
            if(offset == b.info.numkeys - 1){
                superblock.info.freelist = node;
            } else{
                for(SIZE_T j = offset; j < b.info.numkeys -1; j++){
                    KEY_T tmpKey;
                    VALUE_T tmpVal;
                    b.GetKey(j,tmpKey);
                    b.SetKey(j+1, tmpKey);
                    b.GetVal(j, tmpVal);
                    b.SetVal(j+1, tmpVal);
                }
            }
            return b.Serialize(buffercache, node);
        }else{
            rc = DeallocateNode(node);
            if (rc) return rc; 
            return  b.Serialize(buffercache, node); 
            
         } 
        }
        } 
        return ERROR_NONEXISTENT;
        break;
    default:
        return ERROR_INSANE;
    }
  return ERROR_NOERROR;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) { 
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "di withree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::RangeQuery(const KEY_T &minKey, const KEY_T &maxKey, std::vector<VALUE_T> &values){

  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  VALUE_T value;
  //check if 1) b is a tree? 2) in order? 3) balanced? 4) Does each node have a valid
  //use ratio?

  rc= b.Unserialize(buffercache,superblock.info.rootnode);
  std::vector<KEY_T> allKeys;
  std::vector<VALUE_T> allValues;
  if(rc) return rc;
  rc  = InOrderCheck(superblock.info.rootnode, allKeys, allValues);
  if(rc) return rc;
  for(std::vector<VALUE_T>::iterator it = allKeys.begin();it != allKeys.end();it++){
    if( (minKey < (*it) || minKey == (*it)) && ((*it) < maxKey || (*it) == maxKey)){
        cerr << *(allValues.begin() + (it - allKeys.begin())) << endl;
        VALUE_T ret = *(allValues.begin() + (it - allKeys.begin()));
        values.push_back(ret);
    }   
  
  }
  return b.Serialize(buffercache, superblock.info.rootnode);
  //return ERROR_NOERROR;
}

ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  VALUE_T value;
  //check if 1) b is a tree? 2) in order? 3) balanced? 4) Does each node have a valid
  //use ratio?

  rc= b.Unserialize(buffercache,superblock.info.rootnode);
  std::vector<KEY_T> allData;
  std::vector<VALUE_T> values;
  if (rc) return rc;
  rc  = InOrderCheck(superblock.info.rootnode, allData, values);
  for(std::vector<KEY_T>::iterator it = allData.begin(); it != allData.end() -1;it++){
      //for (i=0; i <b.info.keysize; i++){   
        // cerr << (*it).data[i];
      //}
      std::vector<KEY_T>::iterator next = it + 1;
      //for (i=0; i <b.info.keysize; i++){   
        // cerr << (*next).data[i];
      //}

      if((*next)  < (*it) )
        return ERROR_INSANE;
  } 
  int height = 0;
  //rc = GetHeight(superblock.info.rootnode, height);
  //cerr << "height is " << height << endl;
  int maxheight = 0;
  int minheight = 10000;
  for(offset = 0; offset<=b.info.numkeys;offset++){
      height = 0;
      rc = b.GetPtr(offset, ptr);
      if(rc) return rc;
      rc = GetHeight(ptr, height);
      cerr << "height is " << height << endl; 
      if(height > maxheight)
          maxheight = height;
      if(height < minheight)
          minheight = height;
  }
  cerr << "max height is " << maxheight << endl;
  cerr << "min height is " << minheight << endl; 
  if (maxheight - minheight > 1)
      return ERROR_INSANE;
  
  rc = UseRatioCheck(superblock.info.rootnode);
  if (rc!=ERROR_NOERROR) { 
      return rc;
  }
 
  return b.Serialize(buffercache, superblock.info.rootnode);
  //return ERROR_NOERROR;
}
ERROR_T BTreeIndex::UseRatioCheck(const SIZE_T &node)const{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  VALUE_T value; 
  rc= b.Unserialize(buffercache,node);
  if (rc!=ERROR_NOERROR) { 
      return rc;
  }
  if ( b.info.nodetype == BTREE_ROOT_NODE) {
    // at least 2 ptrs at root node
      if(b.info.numkeys < 1)
        return ERROR_INSANE;
      for(offset = 0; offset <= b.info.numkeys;offset++){
        rc = b.GetPtr(offset,ptr);
        cerr << ptr << endl;
        if(rc) return rc;
        rc = UseRatioCheck(ptr);
        if(rc) return rc;
      }
  } else if (b.info.numkeys>0 && b.info.nodetype == BTREE_INTERIOR_NODE){
    
      int n = floor((b.info.blocksize - b.info.valuesize)/(b.info.valuesize+b.info.keysize));
      cerr << "n = " << n<<endl;
      cerr << "num keys at interior node " << b.info.numkeys << endl;

      cerr << "num slots at interior node " << b.info.GetNumSlotsAsInterior() << endl;
      if( floor((n + 1)/2) >= b.info.GetNumSlotsAsInterior())
          return ERROR_INSANE;
      for(offset = 0; offset<=b.info.numkeys;offset++){
          rc = b.GetPtr(offset,ptr);
          if (rc) return rc;
          rc = UseRatioCheck(ptr);
          if (rc) return rc;
    } 
  }else if(b.info.numkeys>0 && b.info.nodetype == BTREE_LEAF_NODE){
      int n = floor((b.info.blocksize - b.info.valuesize)/(b.info.valuesize+b.info.keysize));
      cerr << "n = " << n<<endl;
      cerr << "num keys " << b.info.numkeys << endl; 
      cerr << "num slots interior " << b.info.GetNumSlotsAsInterior() << endl;
      cerr << "num slots leaf " << b.info.GetNumSlotsAsLeaf() << endl;
      if(floor((n+1)/2) >=  b.info.GetNumSlotsAsLeaf()) 
         return ERROR_INSANE;
  
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::GetHeight(const SIZE_T &node, int &height)const{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  VALUE_T value; 
  rc= b.Unserialize(buffercache,node);
  if (rc!=ERROR_NOERROR) { 
      return rc;
  }
  if (b.info.numkeys>0 && b.info.nodetype != BTREE_LEAF_NODE) { 
    for (offset=0;offset<=b.info.numkeys;offset++) { 
      rc=b.GetPtr(offset,ptr);
      if (rc) { return rc; }
      cerr << "getting ptr " << ptr << endl;
      int tmpHeight;
      rc=GetHeight(ptr, tmpHeight);
      if(tmpHeight > height)
          height = tmpHeight;
      if (rc) { return rc; }
    }
  }
 if (b.info.numkeys>0 && b.info.nodetype == BTREE_LEAF_NODE) {
      height++;
  
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::InOrderCheck(const SIZE_T &node, std::vector<KEY_T> &allData, std::vector<VALUE_T> &values ) const{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset, i;
  VALUE_T value; 
  rc= b.Unserialize(buffercache,node);
  if (rc!=ERROR_NOERROR) { 
      return rc;
  }
  if (b.info.numkeys>0 && b.info.nodetype != BTREE_LEAF_NODE) { 
    
    for (offset=0;offset<=b.info.numkeys;offset++) { 
      //rc=b.GetKey(offset,testkey);
      //if (rc) {  return rc; }
      rc=b.GetPtr(offset,ptr);
      if (rc) { return rc; }
      cerr << "getting ptr " << ptr << endl;

      rc=InOrderCheck(ptr, allData, values);
      if (rc) { return rc; }
    }

 
  } else if(b.info.numkeys>0 && b.info.nodetype == BTREE_LEAF_NODE){
      for( offset= 0; offset < b.info.numkeys; offset++){
      rc = b.GetVal(offset, value);
      rc = b.GetKey(offset, testkey);
      cerr << "getting key ";
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	   cerr  << testkey.data[i];
	}
      cerr << " with value ";
      for (i=0;i<b.info.valuesize;i++) { 
	cerr << value.data[i] ;
      }
      allData.push_back(testkey);
      values.push_back(value);
      cerr << endl;
  }
  }
  return ERROR_NOERROR;
}
  /*if (b.info.numkeys>0) {    
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc = b.GetPtr(offset, ptr);
      if(rc) {return rc;}
      rc = BalancedCheck(ptr);
      

    }
  }
  */


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
    BTreeDisplayType display_type = BTREE_SORTED_KEYVAL;
    os << "digraph tree {\n";
    ERROR_T rc = DisplayInternal(superblock.info.rootnode, os, display_type);
    os << "} \n";
    return os;


}




