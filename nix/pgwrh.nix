{ lib, postgresql, postgresqlBuildExtension, openssl, libkrb5 }:

assert lib.versions.major postgresql.version == "18";
postgresqlBuildExtension {
  pname = "pgwrh";
  version = "1.0.0-alpha1";
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.intersection
      (lib.fileset.unions [ ../Makefile ../LICENSE ../pgwrh ../pgwrh_ui ../pgwrh_fdw ../pgwrh_wait ])
      (lib.fileset.fileFilter
        (file: !(lib.any file.hasExt [ "o" "so" "dylib" "bc" ]) && file.name != ".DS_Store")
        ../.);
  };
  enableUpdateScript = false;
  buildInputs = [ openssl libkrb5 ];
  makeFlags = [ "PG_CONFIG=${postgresql.pg_config}/bin/pg_config" "with_llvm=no" ];
  enableParallelBuilding = true;
  postInstall = ''
    install -Dm644 LICENSE "$out/share/doc/pgwrh/LICENSE"
    install -Dm644 pgwrh_fdw/COPYRIGHT "$out/share/doc/pgwrh/FDW-COPYRIGHT"
  '';
  meta = {
    description = "Sharding and replica read consistency for PostgreSQL 18";
    homepage = "https://github.com/mkleczek/pgwrh";
    license = [ lib.licenses.agpl3Plus lib.licenses.agpl3Only lib.licenses.postgresql lib.licenses.bsd0 ];
    platforms = postgresql.meta.platforms;
  };
}
