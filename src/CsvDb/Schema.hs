{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

-- | This module should be imported qualified
module CsvDb.Schema
  ( Options(..)
  , opts
  , cmd
  , TypeStats
  , getFirstRow
  , getTypeStats
  , inferSchemaConservative
  , createTableQueryPostgres
  ) where

------------------------------------------------------------------------------
import           Control.Error
import           Control.Monad.Trans.Resource
import           Data.ByteString (ByteString)
import           Data.Conduit
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.List as C
import           Data.CSV.Conduit
import           Data.List
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Maybe
import           Data.Monoid
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Text.Encoding
import           Data.Text.Read
import           Data.Word
import           Options.Applicative
import           Text.PrettyPrint.ANSI.Leijen (string, Doc)
import           Text.Printf
------------------------------------------------------------------------------


------------------------------------------------------------------------------
-- Not prefixing these field names because they should not be exported.
data Options = Options
    { optFilename :: String
    , optNoStrip :: Bool
    }


opts :: ParserInfo Options
opts = info sample
    ( fullDesc <> progDescDoc (Just desc))


desc :: Doc
desc = string "Infer schema"


sample :: Parser Options
sample = Options
  <$> argument (maybeReader Just)
      ( metavar "<filename>"
     <> help "Name of CSV file" )
  <*> switch
      ( long "no-strip"
     <> help "Don't strip leading and trailing whitespace from all fields" )

stripBS :: ByteString -> ByteString
stripBS bs = either (const bs) (encodeUtf8 . T.strip) $ decodeUtf8' bs


------------------------------------------------------------------------------
-- | Reads CSV file and returns a TypeStats data structure which has
-- information that can be used to infer a schema that fits the data.
getTypeStats :: Options -> IO TypeStats
getTypeStats Options{..} = do
    let f = if optNoStrip then id else stripBS
    runResourceT (processor optFilename f)

getFirstRow :: String -> IO (Maybe (Row ByteString))
getFirstRow file = runResourceT (CB.sourceFile file $= intoCSV defCSVSettings $$ C.head)

cmd :: Options -> IO ()
cmd o@Options{..} = do
    res <- getTypeStats o
    Just headerRow <- getFirstRow optFilename
    print headerRow
    printf "Max number of parsed rows: %d\n"
           (M.foldl' max 0 $ foldl' max 0 <$> res)
    putStrLn $ prettyTypeStats res
    let schema = inferSchemaConservative res
    putStrLn $ createTableQueryPostgres schema headerRow "my_table"

------------------------------------------------------------------------------
processor :: FilePath -> (ByteString -> ByteString) -> ResourceT IO TypeStats
processor file f =
    CB.sourceFile file $=
    intoCSV defCSVSettings $=
    C.map (fmap f) $$
    C.fold makeSchema mempty

data SqlType
  = SqlBool
  | SqlInt
  | SqlDecimal
  | SqlDate
  | SqlTime
  | SqlDateTime
  | SqlText
  | SqlBytes
  deriving (Eq,Ord,Show,Read,Enum,Bounded)

prettySqlType :: SqlType -> String
prettySqlType SqlBool = "bool"
prettySqlType SqlInt = "int"
prettySqlType SqlDecimal = "decimal"
prettySqlType SqlDate = "date"
prettySqlType SqlTime = "time"
prettySqlType SqlDateTime = "datetime"
prettySqlType SqlText = "text"
prettySqlType SqlBytes = "bytes"

parseBool :: ByteString -> Maybe Bool
parseBool = either (const Nothing) (go . T.toLower) . decodeUtf8'
  where
    go "false" = Just False
    go "true" = Just True
    go "f" = Just False
    go "t" = Just True
    go "off" = Just False
    go "on" = Just True
    go "0" = Just False
    go "1" = Just True
    go _ = Nothing

isBool :: ByteString -> Bool
isBool = isJust . parseBool

parseInteger :: ByteString -> Maybe Integer
parseInteger = either (const Nothing) go . decodeUtf8'
  where
    go t =
      case signed decimal t of
        Left _ -> Nothing
        -- The decimal function doesn't require that the whole string be an
        -- integer.  Only that the string starts with an integer.  So we can
        -- only return true if the trailing string was empty.
        Right (n, "") -> Just n
        Right _ -> Nothing

isInt :: ByteString -> Bool
isInt = isJust . parseInteger

parseDouble :: Text -> Maybe Double
parseDouble t =
  case double t of
    Left _ -> Nothing
    -- The double function doesn't require that the whole string be an
    -- integer.  Only that the string starts with an integer.  So we can
    -- only return true if the trailing string was empty.
    Right (x, "") -> Just x
    Right _ -> Nothing

isDouble :: ByteString -> Bool
isDouble = either (const False) (isJust . parseDouble) . decodeUtf8'

parseText :: ByteString -> Maybe Text
parseText = hush . decodeUtf8'

isText :: ByteString -> Bool
isText = isJust . parseText

------------------------------------------------------------------------------
-- | The outer map key is the field name.  The inner map holds all the types
-- that have successfully parsed for that field and the counts of how many
-- rows parsed to that type successfully.
type TypeStats = Map ByteString (Map SqlType Word64)

prettyTypeStats :: TypeStats -> String
prettyTypeStats = concatMap makeSingle . M.toList
  where
    makeSingle (k,v) = unlines
      [ replicate 78 '-'
      , "Field: " <> (T.unpack $ decodeUtf8 k)
      , ""
      , makeFieldGraph v
      ]

padRight :: Int -> String -> String
padRight n s = if len < n then s <> (replicate (n-len) ' ') else s
  where
    len = length s

makeFieldGraph :: Map SqlType Word64 -> String
makeFieldGraph m = unlines $ map makeRow sqlTypes
  where
    sqlTypes = [minBound..maxBound]
    maxVal = M.foldl' max 0 m
    binSize = maxVal `div` 68
    makeRow ty = printf "%8s |%s (%d)"
                        (prettySqlType ty)
                        (padRight 68 $ valRep $ M.lookup ty m)
                        (fromMaybe 0 $ M.lookup ty m)
    valRep Nothing = ""
    valRep (Just 0) = ""
    valRep (Just n)
      | n < binSize = "."
      | otherwise = replicate (fromIntegral $ n `div` binSize) '#'

addToSchema :: TypeStats -> (ByteString, ByteString) -> TypeStats
addToSchema s (field, val) = theFunc s
  where
    theFunc = (if isBool val then M.alter (f SqlBool) field else id)
            . (if isInt val then M.alter (f SqlInt) field else id)
            . (if isDouble val then M.alter (f SqlDecimal) field else id)
            -- TODO Date and time parsing not implemented yet
            . (if isText val then M.alter (f SqlText) field else id)
            . (M.alter (f SqlBytes) field)
    f sty Nothing = Just $ M.singleton sty 1
    f sty (Just m) = Just $ M.alter g sty m
    g Nothing = Just 1
    g (Just n) = Just (n+1)


makeSchema :: TypeStats -> MapRow ByteString -> TypeStats
makeSchema s r = foldl' addToSchema s $ M.toList r

------------------------------------------------------------------------------
-- | Converts TypeStats into the most conservative schema that should be
-- guaranteed to work with all the data.
inferSchemaConservative :: TypeStats -> Map ByteString SqlType
inferSchemaConservative = fmap inferFieldTypeConservative

------------------------------------------------------------------------------
inferFieldTypeConservative :: Map SqlType Word64 -> SqlType
inferFieldTypeConservative m =
    fromMaybe SqlBytes $ getFirst $ mconcat $
      map (First . f) [minBound..maxBound]
  where
    f ty = if M.lookup ty m == Just highestCount
             then Just ty
             else Nothing
    highestCount = M.foldl' max 0 m

------------------------------------------------------------------------------
-- | Construct a Postgres-compatible SQL query that will create a table to
-- hold the data in a CSV file.
createTableQueryPostgres
    :: Map ByteString SqlType
    -- ^ Table schema
    -> [ByteString]
    -- ^ The order of the columns
    -> String
    -- ^ Table name (must be properly quoted for SQL)
    -> String
    -- ^ SQL query
createTableQueryPostgres schema fields table = unlines
    [ printf "CREATE TABLE %s (" table
    , intercalate ", \n" $ map (\f -> mkField (f, fromJust $ M.lookup f schema)) fields
    , ");"
    ]
  where
    mkField (f,t) = unwords [" ", T.unpack (decodeUtf8 f), sqlTypePostgres t]

sqlTypePostgres :: SqlType -> String
sqlTypePostgres SqlBool = "boolean"
sqlTypePostgres SqlInt = "bigint"
sqlTypePostgres SqlDecimal = "double precision"
sqlTypePostgres SqlDate = "date"
sqlTypePostgres SqlTime = "time"
sqlTypePostgres SqlDateTime = "timestamp"
sqlTypePostgres SqlText = "text"
sqlTypePostgres SqlBytes = "bytea"
