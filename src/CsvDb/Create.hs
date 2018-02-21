{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

-- | This module should be imported qualified
module CsvDb.Create
  ( Options
  , opts
  , cmd
  ) where

------------------------------------------------------------------------------
import           Data.Monoid
import qualified Data.Text as T
import           Data.Text.Encoding
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Types
import           Options.Applicative
import           Text.PrettyPrint.ANSI.Leijen (string, Doc)
import           Text.Printf
------------------------------------------------------------------------------
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

connectInfoParser :: Parser ConnectInfo
connectInfoParser = ConnectInfo
  <$> strOption
      ( short 'h'
     <> long "host"
     <> value "localhost"
     <> help "Database host" )
  <*> option auto
      ( short 'p'
     <> long "port"
     <> value 5432
     <> help "Database port" )
  <*> strOption
      ( short 'u'
     <> long "user"
     <> help "Database user" )
  <*> strOption
      ( short 'w'
     <> long "password"
     <> value ""
     <> help "Database password" )
  <*> strOption
      ( short 'd'
     <> long "dbname"
     <> help "Database name" )

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
     <> help "Strip leading and trailing whitespace from all fields before inferring schema" )
  <*> strOption
      ( short 't'
     <> long "table"
     <> help "Database table name" )
  <*> connectInfoParser

cmd :: Options -> IO ()
cmd Options{..} = do
    conn <- connect optDbConn
    Just headerRow <- Schema.getFirstRow optFilename
    tstats <- Schema.getTypeStats (Schema.Options optFilename optNoStrip)
    let schema = Schema.inferSchemaConservative tstats
        qstr = Schema.createTableQueryPostgres schema headerRow optTableName
    res <- execute_ conn (Query $ encodeUtf8 $ T.pack qstr)
    printf "%d rows inserted\n" res

