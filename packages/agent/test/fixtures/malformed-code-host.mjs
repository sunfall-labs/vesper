process.stdin.once('data', () => {
  process.stdout.write('{"_tag":"Output","value":42}\n', () => {
    process.exitCode = 0;
    process.stdin.destroy();
  });
});
