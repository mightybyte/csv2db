{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TupleSections       #-}

-- | This module should be imported qualified
module CsvDb.Import
  ( Options
  , opts
  , cmd
  ) where

------------------------------------------------------------------------------
import           Control.Monad
import           Data.Int
import           Data.Monoid
import qualified Data.Text as T
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.SqlQQ
import           Database.PostgreSQL.Simple.Types
import           Options.Applicative
import           System.Directory
import           System.FilePath
import           System.Random
import           Text.PrettyPrint.ANSI.Leijen (string, Doc)
import           Text.Printf
------------------------------------------------------------------------------
import           CsvDb.Common
import qualified CsvDb.Clean as Clean
import qualified CsvDb.Create as Create
------------------------------------------------------------------------------


------------------------------------------------------------------------------
-- Not prefixing these field names because they should not be exported.
data Options = Options
    { optFilename :: String
    , optNoClean :: Bool
    , optNoStrip :: Bool
    , optTableName :: String
    , optDbConn :: Maybe ConnectInfo
    , optDbConnStr :: Maybe String
    }

opts :: ParserInfo Options
opts = info sample
    (fullDesc <> progDescDoc (Just desc) <> footerDoc (Just verboseDesc))


desc :: Doc
desc = string "Import CSV into database"

verboseDesc :: Doc
verboseDesc = string "Assumes the CSV file has a header and uses that header to determine field names."

sample :: Parser Options
sample = Options
  <$> argument (maybeReader Just)
      ( metavar "<filename>"
     <> help "Name of CSV file" )
  <*> switch
      ( long "no-clean"
     <> help "Don't clean the CSV file before importing" )
  <*> switch
      ( long "no-strip"
     <> help "Don't strip leading and trailing whitespace from all fields" )
  <*> strOption
      ( short 't'
     <> long "table"
     <> help "Database table name" )
  <*> optional connectInfoParser
  <*> optional (strOption
      ( long "connstr"
     <> help "Database connection string" ))

copyCsv
    :: Connection
    -> String
    -- ^ Table name
    -> String
    -- ^ Full absolute path to the CSV file
    -> IO Int64
copyCsv conn table file =
    execute conn [sql|
      COPY ? FROM ? WITH (FORMAT csv, HEADER TRUE);
    |] (Identifier $ T.pack table, file)


doesTableExist :: Connection -> String -> IO Bool
doesTableExist conn table = do
    [Only exists] <- query conn [sql|
      SELECT EXISTS (
         SELECT 1
         FROM   pg_catalog.pg_class c
         JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE  c.relname = ?
         AND    c.relkind = 'r'
         );
    |] [table]
    return exists

------------------------------------------------------------------------------
cmd :: Options -> IO ()
cmd Options{..} = do
    conn <- decideConnectionMethod optDbConn optDbConnStr
    printf "Checking whether table %s exists...\n" optTableName
    tableExists <- doesTableExist conn optTableName
    unless tableExists $ do
      putStrLn "Table does not exist.  Inferring schema and creating table..."
      void $ Create.doCreate conn optFilename optNoStrip optTableName
    file <- if optNoClean
              then if isAbsolute optFilename
                     then return optFilename
                     else do
                       dir <- getCurrentDirectory
                       return (dir </> optFilename)
              else do
                tmpdir <- getTemporaryDirectory
                g <- newStdGen
                let randString = take 16 $ randomRs ('a', 'z') g
                    tmpFile = tmpdir </> ("csv2db-tmp-" ++ randString) <.> "csv"
                printf "Cleaning data and putting it in %s...\n" tmpFile
                Clean.cmd (Clean.Options optFilename tmpFile optNoStrip)
                return tmpFile
    putStrLn "Loading data into table..."
    numRows <- copyCsv conn optTableName file
    printf "%d rows inserted\n" numRows
    unless optNoClean $ do
      printf "Removing %s\n" file
      removeFile file

