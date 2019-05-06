import qualified Tests.BigBed as BB
import Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "Main"
    [ BB.tests
    ]
