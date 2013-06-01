module Main where

import Data.List
import System.IO
import Network

import Thrift.Transport.Handle
import Thrift.Transport.Framed
import Thrift.Protocol.Binary

import Paxos_Types
import PaxosProposerService_Client as Proposer
import qualified Data.ByteString.Lazy.Char8 as C

main :: IO ()
main = do
  t <- hOpen ("127.0.0.1", PortNumber 9081)
  ft <- openFramedTransport t
  let p = BinaryProtocol ft
  let m = Value { f_Value_cmd = Just $ C.pack "test" }
  result <- propose (p,p) m
  print result
  tClose t
