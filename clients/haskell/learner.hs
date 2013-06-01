module Main where

import Data.List
import System.IO
import Network

import Thrift.Transport.Handle
import Thrift.Transport.Framed
import Thrift.Protocol.Binary

import Paxos_Types
import PaxosLearnerService_Client as Learner

listen p i = do
  result <- deliver p i
  print result
  listen p i
  
main :: IO ()
main = do
  t <- hOpen ("127.0.0.1", PortNumber 9093)
  ft <- openFramedTransport t
  let p = BinaryProtocol ft
  listen (p,p) 10000
  tClose t
