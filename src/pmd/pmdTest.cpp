
#include "ossMmapFile.hpp"
#include "ossPrimitiveFileOp.hpp"


int main ()
{
   void * pAddress = NULL;
   ossMmapFile omf ;
   const char * fileName = "testMmap.txt" ;
   ossPrimitiveFileOp of ;   

   of.Open( fileName , OSS_PRIMITIVE_FILE_OP_OPEN_ALWAYS ) ;
   of.seekToOffset ( (offsetType) 20) ;
   of.Write( (char *)" ", 0) ;
   of.Close() ;   

   omf.open ( fileName , 0 ) ;
   omf.map ( 0, 10 , (void **)&pAddress ) ;
   strncpy ( (char *)pAddress, "hello", 6 ) ;
   omf.close () ;
done :
   return 0 ;
error :
   goto done ;
}
