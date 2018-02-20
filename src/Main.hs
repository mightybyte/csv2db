{-# LANGUAGE OverloadedStrings #-}
module Main where

------------------------------------------------------------------------------
import           Data.Monoid
import           Options.Applicative
import           System.IO
import           Text.PrettyPrint.ANSI.Leijen (Doc, string)
------------------------------------------------------------------------------
import qualified CsvDb.Import as Import
import qualified CsvDb.Schema as Schema
------------------------------------------------------------------------------

data Commands
  = SchemaCmd Schema.Options
  | ImportCmd Import.Options

------------------------------------------------------------------------------
main :: IO ()
main = do
    hSetBuffering stdout NoBuffering

    let commands = commandGroup "Commands"
          <> command "schema" (SchemaCmd <$> Schema.opts)
          <> command "import" (ImportCmd <$> Import.opts)

    let optParser :: Parser Commands
        optParser = hsubparser commands

        opts :: ParserInfo Commands
        opts = info (helper <*> optParser)
          ( progDesc (unwords
              ["Tool for importing CSV files into a database"
              ])
         <> header "csv2db" )

    c <- execParser opts
    case c of
      SchemaCmd o -> Schema.cmd o
      ImportCmd o -> Import.cmd o
