{-# LANGUAGE OverloadedStrings   #-}
module CsvDb.Common where

------------------------------------------------------------------------------
import           Control.Error
import           Data.ByteString (ByteString)
import           Data.Monoid
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Text.Encoding
import           Data.Text.Read
import           Database.PostgreSQL.Simple
import           Options.Applicative
------------------------------------------------------------------------------

stripBS :: ByteString -> ByteString
stripBS bs = either (const bs) (encodeUtf8 . T.strip) $ decodeUtf8' bs

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
