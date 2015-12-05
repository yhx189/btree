#include <stdlib.h>
#include "btree.h"
#include <vector>
void usage() 
{
  cerr << "usage: btree_range_query filestem cachesize minkey maxkey\n";
}


int main(int argc, char **argv)
{
  char *filestem;
  SIZE_T cachesize;
  SIZE_T superblocknum;
  //char *key;

  if (argc!=5) { 
    usage();
    return -1;
  }

  filestem=argv[1];
  cachesize=atoi(argv[2]);
  char *minkey=argv[3];
  char *maxkey=argv[4];

  DiskSystem disk(filestem);
  BufferCache cache(&disk,cachesize);
  BTreeIndex btree(0,0,&cache);
  
  ERROR_T rc;


  if ((rc=cache.Attach())!=ERROR_NOERROR) { 
    cerr << "Can't attach buffer cache due to error"<<rc<<endl;
    return -1;
  }

  if ((rc=btree.Attach(0))!=ERROR_NOERROR) { 
    cerr << "Can't attach to index  due to error "<<rc<<endl;
    return -1;
  } else {
    cerr << "Index attached!"<<endl;
    std::vector<VALUE_T> vals;
    if ((rc=btree.RangeQuery(KEY_T(minkey), KEY_T(maxkey), vals))!=ERROR_NOERROR) { 
      cerr <<"Lookup failed: error "<<rc<<endl;
    } else {
      cerr <<"Lookup succeeded\n";
      for(std::vector<VALUE_T>::iterator it = vals.begin(); it!= vals.end(); it++){
        cout << *it << endl;
      }
    }
    if ((rc=btree.Detach(superblocknum))!=ERROR_NOERROR) { 
      cerr <<"Can't detach from index due to error "<<rc<<endl;
      return -1;
    }
    if ((rc=cache.Detach())!=ERROR_NOERROR) { 
      cerr <<"Can't detach from cache due to error "<<rc<<endl;
      return -1;
    }
    cerr << "Performance statistics:\n";
    
    cerr << "numallocs       = "<<cache.GetNumAllocs()<<endl;
    cerr << "numdeallocs     = "<<cache.GetNumDeallocs()<<endl;
    cerr << "numreads        = "<<cache.GetNumReads()<<endl;
    cerr << "numdiskreads    = "<<cache.GetNumDiskReads()<<endl;
    cerr << "numwrites       = "<<cache.GetNumWrites()<<endl;
    cerr << "numdiskwrites   = "<<cache.GetNumDiskWrites()<<endl;
    cerr << endl;
    
    cerr << "total time      = "<<cache.GetCurrentTime()<<endl;

    return 0;
  }
}
  

  
