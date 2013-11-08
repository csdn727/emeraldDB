/*******************************************************************************
 Copyright (C) 2013 SequoiaDB Software Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License, version 3,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program. If not, see <http://www.gnu.org/license/>.
*******************************************************************************/
#include "core.hpp"
#include "command.hpp"
#include "commandFactory.hpp"
#include "pd.hpp"
#include "msg.hpp"

#define ossRoundUpToMultipleX(x,y) (((x)+((y)-1))-(((x)+((y)-1))%(y)))

COMMAND_BEGIN
COMMAND_ADD(COMMAND_INSERT,InsertCommand)
COMMAND_ADD(COMMAND_QUERY,QueryCommand)
COMMAND_ADD(COMMAND_DELETE, DeleteCommand)
COMMAND_ADD(COMMAND_CONNECT,ConnectCommand)
COMMAND_ADD(COMMAND_QUIT, QuitCommand)
COMMAND_ADD(COMMAND_HELP, HelpCommand)
COMMAND_ADD(COMMAND_SNAPSHOT, SnapshotCommand)
COMMAND_END

extern int gQuit;

int ICommand::execute(  ossSocket & sock, std::vector<std::string> & argVec )
{
   return EDB_OK;
}

int ICommand::getError(int code)
{
  switch(code)
   {
      case EDB_OK:
         break;
      case EDB_IO:
         std::cout << "io error is occurred" << std::endl;
         break;
      case EDB_INVALIDARG:
         std::cout << "invalid argument" << std::endl;
         break;
      case EDB_PERM:
         std::cout << "edb_perm" << std::endl;
         break;
      case EDB_OOM:
         std::cout << "edb_oom" << std::endl;
         break;
      case EDB_SYS:
         std::cout << "system error is occurred." << std::endl;
         break;
      case EDB_QUIESCED:
         std::cout << "EDB_QUIESCED" << std::endl;
         break;
      case EDB_NETWORK_CLOSE:
         std::cout << "net work is closed." << std::endl;
         break;
      case EDB_HEADER_INVALID:
         std::cout << "record header is not right." << std::endl;
         break;
      case EDB_IXM_ID_EXIST:
         std::cout << "record key is exist." << std::endl;
         break;
      case EDB_IXM_ID_NOT_EXIST:
         std::cout << "record is not exist" << std::endl;
         break;
      case EDB_NO_ID:
         std::cout << "_id is needed" << std::endl;
         break;
      case EDB_QUERY_INVALID_ARGUMENT:
         std::cout << "invalid query argument" << std::endl;
         break;
      case EDB_INSERT_INVALID_ARGUMENT:
         std::cout <<  "invalid insert argument" << std::endl;
         break;
      case EDB_DELETE_INVALID_ARGUMENT:
         std::cout << "invalid delete argument" << std::endl;
         break;
      case EDB_INVALID_RECORD:
         std::cout << "invalid record string" << std::endl;
         break;
      case EDB_SOCK_NOT_CONNECT:
         std::cout << "sock connection does not exist" << std::endl;
         break;
      case EDB_SOCK_REMOTE_CLOSED:
         std::cout << "remote sock connection is closed" << std::endl;
         break;
      case EDB_MSG_BUILD_FAILED:
         std::cout << "msg build failed" << std::endl;
         break;
      case EDB_SOCK_SEND_FAILD:
         std::cout << "sock send msg faild" << std::endl;
         break;
      case EDB_SOCK_INIT_FAILED:
         std::cout << "sock init failed" << std::endl;
         break;
      case EDB_SOCK_CONNECT_FAILED:
         std::cout << "sock connect remote server failed" << std::endl;
         break;
      default :
         break;
   }
   return code;
}


int ICommand::recvReply( ossSocket & sock )
{
   // define message data length.
   int length = 0;
   int ret = EDB_OK;

   // fill receive buffer with 0.
   //memset(_pRecvBuf, 0, RECV_BUF_SIZE);
   _pRecvBuf = (char*)malloc( sizeof(char) * RECV_BUF_SIZE) ;
   if ( !_pRecvBuf )
   {
      ret = EDB_OOM ;
      goto error ;
   }
   if( !sock.isConnected() )
   {
      return getError(EDB_SOCK_NOT_CONNECT);
   }
   while(1)
   {
      // receive data from the server.first receive the length of the data.
      ret = sock.recv(_pRecvBuf, sizeof(int));
      if( EDB_TIMEOUT == ret )
      {
         continue;
      }
      else if( EDB_NETWORK_CLOSE == ret )
      {
         return getError(EDB_SOCK_REMOTE_CLOSED);
      }
      else
      {
         break;
      }
   }
   // get the value of length.
   length = *(int*)_pRecvBuf;
   // judge the length is valid or not.
   if(length+1 > RECV_BUF_SIZE)
   {
      //return getError(EDB_RECV_DATA_LENGTH_ERROR);
      PD_LOG ( PDDEBUG,
               "Receive buffer size is to small: %d vs %d, increasing..",
               RECV_BUF_SIZE, length ) ;
      length = ossRoundUpToMultipleX ( length, RECV_BUF_SIZE ) ;
      if ( length < 0 )
      {
         ret = EDB_INVALIDARG ;
         goto error ;
      }
      free ( _pRecvBuf ) ;
      _pRecvBuf = (char*)malloc ( sizeof(char) * (length+1) ) ;
      if ( !_pRecvBuf )
      {
         ret = EDB_OOM ;
         goto error ;
      }
   }

   // receive data from the server.second receive the last data.
   while(1)
   {
      ret = sock.recv(&_pRecvBuf[sizeof(int)],length-sizeof(int));
      if(ret == EDB_TIMEOUT)
      {
         continue;
      }
      else if(EDB_NETWORK_CLOSE == ret)
      {
         return getError(EDB_SOCK_REMOTE_CLOSED);
      }
      else
      {
         break;
      }
   }
   goto done ;
done :
   return ret ;
error :
   free ( _pRecvBuf ) ;
   goto done ;
}

int ICommand::sendOrder( ossSocket & sock, OnMsgBuild onMsgBuild  )
{
   int ret = EDB_OK;
   bson::BSONObj bsonData;
   int size = SEND_BUF_SIZE;
   char * pSendBuf = _pSendBuf;
   try {
      bsonData = bson::fromjson(_jsonString);
   } catch( std::exception & e) {
      return getError(EDB_INVALID_RECORD);
   }
   //memset(_sendBuf,0, SEND_BUF_SIZE);
   _pSendBuf = (char*)malloc( sizeof(char) * SEND_BUF_SIZE ) ;
   if ( !_pSendBuf )
   {
      ret = EDB_OOM ;
      goto error ;
   }   
   pSendBuf = _pSendBuf;
   ret = onMsgBuild(&pSendBuf, &size, bsonData);
   if(ret)
   {
      return getError(EDB_MSG_BUILD_FAILED);
   }
   ret = sock.send( pSendBuf, *(int*)pSendBuf );
   if(ret)
   {
      return getError(EDB_SOCK_SEND_FAILD);
   }
   goto done ;
done :
   return ret ;
error :
   free ( _pSendBuf ) ;
   goto done ;
}

int ICommand::sendOrder( ossSocket & sock, int opCode )
{
   int ret = EDB_OK;
   char * pSendBuf = _pSendBuf;
   MsgHeader *header = NULL ;
   //memset(_sendBuf, 0, SEND_BUF_SIZE);
   _pSendBuf = (char*)malloc( sizeof(char) * SEND_BUF_SIZE ) ;
   if ( !_pSendBuf )
   {
      ret = EDB_OOM ;
      goto error ;
   }
   pSendBuf = _pSendBuf;
   header = (MsgHeader*)pSendBuf;
   header->messageLen = sizeof(MsgHeader);
   header->opCode = opCode;
   ret = sock.send(pSendBuf, *(int*)pSendBuf);
   goto done ;
done :
   return ret;
error :
   free ( _pSendBuf ) ;
   goto done ;
}

/******************************InsertCommand**********************************************/
int InsertCommand::handleReply()
{
   MsgReply * msg = (MsgReply*)_pRecvBuf;
   int returnCode = msg->returnCode;
   int ret = getError(returnCode);
   return ret;
}

int InsertCommand::execute( ossSocket & sock, std::vector<std::string> & argVec )
{
   int rc = EDB_OK;
   if( argVec.size() <1 )
   {
      return getError(EDB_INSERT_INVALID_ARGUMENT);
   }
   _jsonString = argVec[0];
     if( !sock.isConnected() )
   {
      return getError(EDB_SOCK_NOT_CONNECT);
   }

   rc = sendOrder( sock, msgBuildInsert );
   PD_RC_CHECK ( rc, PDERROR, "Failed to send order, rc = %d", rc ) ;

   rc = recvReply( sock );
   PD_RC_CHECK ( rc, PDERROR, "Failed to receive reply, rc = %d", rc ) ;
   rc = handleReply();
   PD_RC_CHECK ( rc, PDERROR, "Failed to receive reply, rc = %d", rc ) ;
done :
   return rc;
error :
   goto done ;
}

/******************************QueryCommand**********************************************/
int QueryCommand::handleReply()
{
   MsgReply * msg = (MsgReply*)_pRecvBuf;
   int returnCode = msg->returnCode;
   int ret = getError(returnCode);
   if(ret)
   {
      return ret;
   }
   if ( msg->numReturn )
   {
      bson::BSONObj bsonData = bson::BSONObj( &(msg->data[0]) );
      std::cout << bsonData.toString() << std::endl;
   }
   return ret;
}

int QueryCommand::execute( ossSocket & sock, std::vector<std::string> & argVec )
{
   int rc = EDB_OK;
   if( argVec.size() <1 )
   {
      return getError(EDB_QUERY_INVALID_ARGUMENT);
   }
   _jsonString = argVec[0];
   if( !sock.isConnected() )
   {
      return getError(EDB_SOCK_NOT_CONNECT);
   }

   rc = sendOrder( sock, msgBuildQuery );
   PD_RC_CHECK ( rc, PDERROR, "Failed to send order, rc = %d", rc ) ;
   rc = recvReply( sock );
   PD_RC_CHECK ( rc, PDERROR, "Failed to receive reply, rc = %d", rc ) ;
   rc = handleReply();
   PD_RC_CHECK ( rc, PDERROR, "Failed to receive reply, rc = %d", rc ) ;
done :
   return rc;
error :
   goto done ;
}

/******************************DeleteCommand**********************************************/
int DeleteCommand::handleReply()
{
   MsgReply * msg = (MsgReply*)_pRecvBuf;
   int returnCode = msg->returnCode;
   int ret = getError(returnCode);
   return ret;
}

int DeleteCommand::execute( ossSocket & sock, std::vector<std::string> & argVec )
{
   int rc = EDB_OK;
   if( argVec.size() < 1 )
   {
      return getError(EDB_DELETE_INVALID_ARGUMENT);
   }
   _jsonString = argVec[0];
   if( !sock.isConnected() )
   {
      return getError(EDB_SOCK_NOT_CONNECT);
   }
   rc = sendOrder( sock, msgBuildDelete );
   PD_RC_CHECK ( rc, PDERROR, "Failed to send order, rc = %d", rc ) ;
   rc = recvReply( sock );
   PD_RC_CHECK ( rc, PDERROR, "Failed to receive reply, rc = %d", rc ) ;
   rc = handleReply();
   PD_RC_CHECK ( rc, PDERROR, "Failed to receive reply, rc = %d", rc ) ;
done :
   return rc;
error :
   goto done ;
}

/******************************ConnectCommand****************************************/
int ConnectCommand::execute( ossSocket & sock, std::vector<std::string> & argVec )
{
   int ret = EDB_OK;
   _address = argVec[0];
   _port = atoi(argVec[1].c_str());
   sock.close();
   sock.setAddress(_address.c_str(), _port);
   ret = sock.initSocket();
   if(ret)
   {
      return getError(EDB_SOCK_INIT_FAILED);
   }
   ret = sock.connect();
   if(ret)
   {
      return getError(EDB_SOCK_CONNECT_FAILED);
   }
   sock.disableNagle();
   return ret;
}
/******************************QuitCommand**********************************************/
int QuitCommand::handleReply()
{
   int ret = EDB_OK;
   gQuit = 1;
   return ret;
}

int QuitCommand::execute( ossSocket & sock, std::vector<std::string> & argVec )
{
   int ret = EDB_OK;
   if( !sock.isConnected() )
   {
      return getError(EDB_SOCK_NOT_CONNECT);
   }
   ret = sendOrder( sock, OP_DISCONNECT );
   sock.close();
   ret = handleReply();
   return ret;
}
/******************************HelpCommand**********************************************/
int HelpCommand::execute( ossSocket & sock, std::vector<std::string> & argVec )
{
   int ret = EDB_OK;
   printf("List of classes of commands:\n\n");
   printf("%s [server] [port]-- connecting emeralddb server\n", COMMAND_CONNECT);
   printf("%s -- sending a insert command to emeralddb server\n", COMMAND_INSERT);
   printf("%s -- sending a query command to emeralddb server\n", COMMAND_QUERY);
   printf("%s -- sending a delete command to emeralddb server\n", COMMAND_DELETE);
   printf("%s [number]-- sending a test command to emeralddb server\n", COMMAND_TEST);
   printf("%s -- providing current number of record inserting\n", COMMAND_SNAPSHOT);
   printf("%s -- quitting command\n\n", COMMAND_QUIT);
   printf("Type \"help\" command for help\n");
   return ret;
}

/*********************************SnapshotCommand******************************************/
int SnapshotCommand::handleReply()
{
   int ret = EDB_OK;
   MsgReply * msg = (MsgReply*)_pRecvBuf;
   int returnCode = msg->returnCode;
   ret = getError(returnCode);
   if(ret)
   {
      return ret;
   }
   bson::BSONObj bsonData = bson::BSONObj( &(msg->data[0]) );
   printf( "insert times is %d\n", bsonData.getIntField("insertTimes") );
   printf( "del times is %d\n", bsonData.getIntField("delTimes") );
   printf( "query times is %d\n", bsonData.getIntField("queryTimes") );
   printf( "server run time is %dm\n", bsonData.getIntField("serverRunTime") );

   return ret;
}

int SnapshotCommand::execute( ossSocket & sock, std::vector<std::string> &argVec)
{
   int rc = EDB_OK;
   if( !sock.isConnected() )
   {
      return getError(EDB_SOCK_NOT_CONNECT);
   }

   rc = sendOrder( sock, OP_SNAPSHOT );
   PD_RC_CHECK ( rc, PDERROR, "Failed to send order, rc = %d", rc ) ;
   rc = recvReply( sock );
   PD_RC_CHECK ( rc, PDERROR, "Failed to receive reply, rc = %d", rc ) ;
   rc = handleReply();
   PD_RC_CHECK ( rc, PDERROR, "Failed to receive reply, rc = %d", rc ) ;
done :
   return rc;
error :
   goto done ;
}

