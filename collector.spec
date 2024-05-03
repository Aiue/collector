Name:    %{getenv:PKG_PREFIX}-%{getenv:PKG_NAME}-%{getenv:PKG_VERSION}%{getenv:PKG_POSTFIX}
Version: %{getenv:PKG_VERSION}
Release: %{getenv:PKG_RELEASE}
Summary: %{getenv:PKG_SUMMARY}
License: %{getenv:PKG_LICENSE}
Source:  %{getenv:PKG_SOURCE}

%global installroot %{getenv:PKG_INSTALLPATH}/%{getenv:PKG_NAME}/%{getenv:PKG_VERSION}%{getenv:PKG_POSTFIX}

%files %{installroot}

#--

%description
Common Crawl data collector.

%install
cp *.md %{buildroot}/%{installroot}
cp *.py %{buildroot}/%{installroot}
cp *.conf %{buildroot}/%{installroot}
