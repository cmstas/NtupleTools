mkdir -p test/
tar -czf package.tar.gz main.exe liblooperBatch.so CORE LinkDef_out_rdict.pcm
cp condor_executable.sh test/
mv package.tar.gz  test/
