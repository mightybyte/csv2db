module CsvDb.Common where

------------------------------------------------------------------------------
import           Data.ByteString (ByteString)
import qualified Data.Text as T
import           Data.Text.Encoding
------------------------------------------------------------------------------

stripBS :: ByteString -> ByteString
stripBS bs = either (const bs) (encodeUtf8 . T.strip) $ decodeUtf8' bs
