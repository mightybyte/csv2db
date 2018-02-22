{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

-- | This module should be imported qualified
module CsvDb.Create
  ( Options(..)
  , opts
  , cmd
  , doCreate
  ) where

------------------------------------------------------------------------------
import           Data.Int
import           Data.Monoid
import qualified Data.Text as T
import           Data.Text.Encoding
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Types
import           Options.Applicative
import           Text.PrettyPrint.ANSI.Leijen (string, Doc)
------------------------------------------------------------------------------
import           CsvDb.Common
import qualified CsvDb.Schema as Schema
------------------------------------------------------------------------------


------------------------------------------------------------------------------
-- Not prefixing these field names because they should not be exported.
data Options = Options
    { optFilename :: String
    , optNoStrip :: Bool
    , optTableName :: String
    , optDbConn :: ConnectInfo
    }

opts :: ParserInfo Options
opts = info sample
    ( fullDesc <> progDescDoc (Just desc))


desc :: Doc
desc = string "Create table"


sample :: Parser Options
sample = Options
  <$> argument (maybeReader Just)
      ( metavar "<filename>"
     <> help "Name of CSV file" )
  <*> switch
      ( long "no-strip"
     <> help "Don't strip leading and trailing whitespace from all fields" )
  <*> strOption
      ( short 't'
     <> long "table"
     <> help "Database table name" )
  <*> connectInfoParser

cmd :: Options -> IO ()
cmd Options{..} = do
    conn <- connect optDbConn
    doCreate conn optFilename optNoStrip optTableName
    return ()

------------------------------------------------------------------------------
-- | Infer schema and create the table
doCreate :: Connection -> String -> Bool -> String -> IO Int64
doCreate conn optFilename optNoStrip optTableName = do
    Just headerRow <- Schema.getFirstRow optFilename
    tstats <- Schema.getTypeStats (Schema.Options optFilename optNoStrip)
    let schema = Schema.inferSchemaConservative tstats
        qstr = Schema.createTableQueryPostgres schema headerRow optTableName
    execute_ conn (Query $ encodeUtf8 $ T.pack qstr)
