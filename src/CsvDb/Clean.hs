{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

-- | This module should be imported qualified
module CsvDb.Clean
  ( Options
  , opts
  , cmd
  ) where

------------------------------------------------------------------------------
import           Control.Monad.Trans.Resource
import           Data.ByteString (ByteString)
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.List as C
import           Data.CSV.Conduit
import           Data.Monoid
import           Options.Applicative
import           Text.PrettyPrint.ANSI.Leijen (string, Doc)
------------------------------------------------------------------------------
import           CsvDb.Common
------------------------------------------------------------------------------


------------------------------------------------------------------------------
-- Not prefixing these field names because they should not be exported.
data Options = Options
    { optInFile :: String
    , optOutFile :: String
    , optNoStrip :: Bool
    }


opts :: ParserInfo Options
opts = info sample
    ( fullDesc <> progDescDoc (Just desc))


desc :: Doc
desc = string "Clean CSV data"


sample :: Parser Options
sample = Options
  <$> strOption
      ( short 'i'
     <> long "infile"
     <> help "Input file" )
  <*> strOption
      ( short 'o'
     <> long "outfile"
     <> help "Output file" )
  <*> switch
      ( long "no-strip"
     <> help "Don't strip leading and trailing whitespace from all fields" )


cmd :: Options -> IO ()
cmd Options{..} = do
    let f = if optNoStrip then id else stripBS
    runResourceT (processor optInFile optOutFile f)


------------------------------------------------------------------------------
processor :: FilePath -> FilePath -> (ByteString -> ByteString) -> ResourceT IO ()
processor inFile outFile f =
    transformCSV defCSVSettings
                 (CB.sourceFile inFile)
                 (C.map (map f))
                 (CB.sinkFile outFile)

