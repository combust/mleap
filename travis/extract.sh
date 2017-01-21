openssl aes-256-cbc -K $encrypted_21bd84d1dece_key -iv $encrypted_21bd84d1dece_iv -in travis/travis.tar -out travis/unsafe.travis.tar -d
tar -xvf travis/unsafe.travis.tar -C travis/
cp travis/unsafe.travis/unsafe.credentials.sbt ./unsafe.credentials.sbt
eval "$(ssh-agent -s)"
ssh-add travis/unsafe.travis/unsafe.mleap.git
