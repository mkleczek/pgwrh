{ fetchurl, python3Packages }:

python3Packages.buildPythonPackage rec {
  pname = "testgres";
  version = "1.10.3";
  format = "setuptools";

  src = fetchurl {
    url = "https://files.pythonhosted.org/packages/source/t/testgres/testgres-${version}.tar.gz";
    hash = "sha256-8AG8bM/Ax3y1ojLVsxKniaTL60GDwbdyXFbbyDxyX6k=";
  };

  nativeBuildInputs = [
    python3Packages.setuptools
  ];

  propagatedBuildInputs = with python3Packages; [
    packaging
    pg8000
    port-for
    psutil
    six
  ];

  doCheck = false;
  pythonImportsCheck = [ "testgres" ];
}
