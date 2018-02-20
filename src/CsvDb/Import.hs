{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

-- | This module should be imported qualified
module CsvDb.Import
  ( Options
  , opts
  , cmd
  ) where

------------------------------------------------------------------------------
import           Data.Monoid
import           Options.Applicative
import           Text.PrettyPrint.ANSI.Leijen (string, Doc)
------------------------------------------------------------------------------


------------------------------------------------------------------------------
-- Not prefixing these field names because they should not be exported.
data Options = Options
    { optFilename :: String
    }


opts :: ParserInfo Options
opts = info sample
    ( fullDesc <> progDescDoc (Just desc))


desc :: Doc
desc = string "Import CSV data"


sample :: Parser Options
sample = Options
  <$> argument (maybeReader Just)
      ( metavar "<filename>"
     <> help "Name of file to import" )


cmd :: Options -> IO ()
cmd Options{..} = do
    putStrLn "Not implemented yet"
