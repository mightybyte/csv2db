# To pin to a specific version of nixpkgs, you can substitute <nixpkgs> with:
# `(builtins.fetchTarball "https://github.com/NixOS/nixpkgs/archive/<nixpkgs_commit_hash>.tar.gz")`
{ pkgs ? import (builtins.fetchTarball "https://github.com/NixOS/nixpkgs/archive/23e418f495ae87cf46c5819846c392ebfb042739.tar.gz") {} }: pkgs.haskellPackages.developPackage
  { root = ./.;
    overrides = self: super:
      { # Don't run a package's test suite
        # foo = pkgs.haskell.lib.dontCheck pkgs.haskellPackages.foo;
        #
        # Don't enforce package's version constraints
        # bar = pkgs.haskell.lib.doJailbreak pkgs.haskellPackages.bar;
        #
        # To discover more functions that can be used to modify haskell
        # packages, run "nix-repl", type "pkgs.haskell.lib.", then hit
        # <TAB> to get a tab-completed list of functions.
      };
    source-overrides =
      { # Use a specific hackage version
        # io-streams = "1.4.0.0";

        # Use a particular commit from github
        csv-conduit = pkgs.fetchFromGitHub
          { owner = "mightybyte";
            repo = "csv-conduit";
            rev = "62ca236012e93e0ab66c1838055dc5fcea3d4b29";
            sha256 = "1hzsr0p75r59bzl9vfdxq0hb3rmw7xy4n8yi7cd28fkaykiby2fz";
          };
      };
  }
