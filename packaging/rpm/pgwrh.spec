%global sname pgwrh
%global upstream_version 1.0.0-alpha1

# PGDG's build system supplies these macros; defaults also allow local builds.
%{!?pgmajorversion:%global pgmajorversion 18}
%{!?pginstdir:%global pginstdir /usr/pgsql-%{pgmajorversion}}
%{!?llvm:%global llvm 1}

# Select the matching bundled FDW and compile the shared wait extension.
%if %{pgmajorversion} != 18 && %{pgmajorversion} != 19
%{error:pgwrh requires PostgreSQL 18 or 19}
%endif

%if %llvm
%global with_llvm_arg %{nil}
%else
%global with_llvm_arg with_llvm=no
%endif

Summary:        Sharding and replica read consistency for PostgreSQL
Name:           %{sname}_%{pgmajorversion}
Version:        1.0.0~alpha1
Release:        1PGDG%{?dist}
License:        AGPL-3.0-or-later AND AGPL-3.0-only AND GPL-3.0-only AND PostgreSQL AND 0BSD
URL:            https://github.com/mkleczek/%{sname}
Source0:        https://github.com/mkleczek/%{sname}/archive/refs/tags/v%{upstream_version}.tar.gz#/%{sname}-%{upstream_version}.tar.gz

BuildRequires:  gcc make python3
BuildRequires:  postgresql%{pgmajorversion}-devel
BuildRequires:  krb5-devel
Requires:       postgresql%{pgmajorversion}-server
Requires:       postgresql%{pgmajorversion}-libs
Requires:       postgresql%{pgmajorversion}-contrib
Requires:       pg_background_%{pgmajorversion} >= 2.0.3

%if 0%{?suse_version} >= 1500
BuildRequires:  libopenssl-3-devel
%else
BuildRequires:  openssl-devel
%endif

%description
pgwrh distributes PostgreSQL shards across logical replicas using weighted
rendezvous hashing. This package includes the pgwrh SQL extension, pgwrh_ui
for a controller console served by external PostgREST, pgwrh_wait
for replication visibility barriers, and pgwrh_fdw for virtual foreign servers
and propagation of transaction settings. The optional pgwrh_gist_extra extension
adds GiST text-array operators using PostgreSQL's btree_gist extension.

Extensions are enabled explicitly with CREATE EXTENSION. The pgwrh_wait module
also requires shared_preload_libraries configuration and a PostgreSQL restart.

%if %llvm
%package llvmjit
Summary:        Just-in-time compilation support for pgwrh
Requires:       %{name}%{?_isa} = %{version}-%{release}
%if 0%{?suse_version} == 1500
BuildRequires:  llvm17-devel clang17-devel
Requires:       llvm17
%endif
%if 0%{?suse_version} == 1600
BuildRequires:  llvm19-devel clang19-devel
Requires:       llvm19
%endif
%if 0%{?amzn}
BuildRequires:  llvm-devel >= 15.0 clang-devel >= 15.0
Requires:       llvm >= 15.0
%endif
%if (0%{?fedora} || 0%{?rhel} >= 8) && !0%{?amzn}
BuildRequires:  llvm-devel >= 19.0 clang-devel >= 19.0
Requires:       llvm >= 19.0
%endif

%description llvmjit
LLVM bitcode for the pgwrh_wait, pgwrh_fdw and pgwrh_gist_extra extensions, used by PostgreSQL's
just-in-time compiler.
%endif

%prep
%setup -q -n %{sname}-%{upstream_version}
cp pgwrh_gist_extra/LICENSE GIST-EXTRA-LICENSE
# A release archive must contain the matching extension and bundled sources.
for extension in pgwrh pgwrh_ui pgwrh_wait pgwrh_gist_extra; do
    grep -Eq "^default_version[[:space:]]*=[[:space:]]*'%{upstream_version}'" "$extension/$extension.control"
done
grep -Eq "^default_version[[:space:]]*=[[:space:]]*'%{upstream_version}'" pgwrh_fdw/%{pgmajorversion}/pgwrh_fdw.control

%build
PATH=%{pginstdir}/bin:$PATH %{__make} %{?_smp_mflags} \
    PG_CONFIG=%{pginstdir}/bin/pg_config %{with_llvm_arg}

%install
PATH=%{pginstdir}/bin:$PATH %make_install \
    PG_CONFIG=%{pginstdir}/bin/pg_config %{with_llvm_arg}

# PGDG keeps extension READMEs under the versioned PostgreSQL prefix.
%{__install} -d %{buildroot}%{pginstdir}/doc/extension
%{__install} -m 644 README.md %{buildroot}%{pginstdir}/doc/extension/README-%{sname}.md
%{__install} -m 644 pgwrh_fdw/README.md %{buildroot}%{pginstdir}/doc/extension/README-pgwrh_fdw.md
%{__install} -m 644 docs/lsn-wait.md %{buildroot}%{pginstdir}/doc/extension/README-pgwrh_wait.md
%{__install} -m 644 pgwrh_gist_extra/README.md %{buildroot}%{pginstdir}/doc/extension/README-pgwrh_gist_extra.md

%check
# Checks staged installs and uninstalls; does not start a database server.
PATH=%{pginstdir}/bin:$PATH %{__make} test-packaging \
    PG_CONFIG=%{pginstdir}/bin/pg_config %{with_llvm_arg}

%files
%defattr(-,root,root,-)
%license LICENSE pgwrh_fdw/%{pgmajorversion}/COPYRIGHT
%license GIST-EXTRA-LICENSE
%doc docs pgwrh_fdw/LICENSING.md pgwrh_fdw/UPSTREAM.md
%doc %{pginstdir}/doc/extension/README-%{sname}.md
%doc %{pginstdir}/doc/extension/README-pgwrh_wait.md
%doc %{pginstdir}/doc/extension/README-pgwrh_fdw.md
%doc %{pginstdir}/doc/extension/README-pgwrh_gist_extra.md
%{pginstdir}/share/extension/pgwrh.control
%{pginstdir}/share/extension/pgwrh--%{upstream_version}.sql
%{pginstdir}/share/extension/pgwrh_ui.control
%{pginstdir}/share/extension/pgwrh_ui--%{upstream_version}.sql
%{pginstdir}/share/pgwrh_ui/
%{pginstdir}/share/extension/pgwrh_wait.control
%{pginstdir}/share/extension/pgwrh_wait--%{upstream_version}.sql
%{pginstdir}/share/extension/pgwrh_fdw.control
%{pginstdir}/share/extension/pgwrh_fdw--%{upstream_version}.sql
%{pginstdir}/share/extension/pgwrh_gist_extra.control
%{pginstdir}/share/extension/pgwrh_gist_extra--%{upstream_version}.sql
%{pginstdir}/lib/pgwrh_wait.so
%{pginstdir}/lib/pgwrh_fdw.so
%{pginstdir}/lib/pgwrh_gist_extra.so

%if %llvm
%files llvmjit
%{pginstdir}/lib/bitcode/pgwrh_wait.index.bc
%{pginstdir}/lib/bitcode/pgwrh_wait/
%{pginstdir}/lib/bitcode/pgwrh_fdw.index.bc
%{pginstdir}/lib/bitcode/pgwrh_fdw/
%{pginstdir}/lib/bitcode/pgwrh_gist_extra.index.bc
%{pginstdir}/lib/bitcode/pgwrh_gist_extra/
%endif

%changelog
* Sun Sep 20 2026 Michal Kleczek <michal@kleczek.org> - 1.0.0~alpha1-1PGDG
- Prepare the 1.0.0-alpha1 release of all four PostgreSQL 18 extensions.
- Include the controller UI, bundled assets, and deployment examples.
- Keep fresh-installation-only packaging with no upgrade scripts.

* Wed Sep 16 2026 Michal Kleczek <michal@kleczek.org> - 0.3.0-1PGDG
- Package the bundled pgwrh, pgwrh_wait, and pgwrh_fdw extensions for PostgreSQL 18.
- Split LLVM bitcode into a matching llvmjit subpackage.
