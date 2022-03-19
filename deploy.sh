#!/bin/sh

# set up oh-my-zsh
cp -r env/.oh-my-zsh ~
cp -r env/.zshrc ~
cp -r env/.zshenv ~

# set up rustup and cargo
cp -r env/.rustup ~
cp -r env/.cargo ~

# set up go
cp env/go1.18.linux-amd64.tar.gz ~
cd ~
tar -xzf go1.18.linux-amd64.tar.gz

mkdir -p ~/.config/bltrader
