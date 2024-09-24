Name:    %{getenv:PKG_PREFIX}-%{getenv:PKG_NAME}-%{getenv:PKG_VERSION}%{getenv:PKG_POSTFIX}
Version: %{getenv:PKG_VERSION}
Release: %{getenv:PKG_RELEASE}
Summary: %{getenv:PKG_SUMMARY}
License: %{getenv:PKG_LICENSE}

%global installroot %{getenv:PKG_INSTALLPATH}/%{getenv:PKG_NAME}/%{getenv:PKG_VERSION}%{getenv:PKG_POSTFIX}
%undefine __brp_mangle_shebangs

%files
%{installroot}
%license %{installroot}/license.txt
%doc %{installroot}/README.md
%config(noreplace) %{installroot}/*.conf
#--

%description
Common Crawl data collector.

%install
mkdir -p %{buildroot}%{installroot}
cp /builds/DRS/common-crawl-collector/*.md %{buildroot}%{installroot}
cp /builds/DRS/common-crawl-collector/*.py %{buildroot}%{installroot}
cp /builds/DRS/common-crawl-collector/*.conf %{buildroot}%{installroot}
chmod 666 %{buildroot}%{installroot}/domains.conf
cp /builds/DRS/common-crawl-collector/license.txt %{buildroot}%{installroot}

