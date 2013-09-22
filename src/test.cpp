#include "ossPrimitiveFileOp.hpp"

int main(void)
{
   ossPrimitiveFileOp _hello;
   _hello.Open ("./_hello.txt", (((unsigned int)1) << 4)) ;
   _hello.Write( "hello!!~", sizeof(("hello!!~")+1) ) ;
}

