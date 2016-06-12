# Must be run as root
if [[ -z $(grep "vm.swappiness = 0" "/etc/sysctl.conf") ]]; then echo "vm.swappiness = 0" >> /etc/sysctl.conf; fi
if [[ -z $(grep "net.ipv6.conf.all.disable_ipv6 = 1" "/etc/sysctl.conf") ]]; then echo "net.ipv6.conf.all.disable_ipv6 = 1" >> /etc/sysctl.conf; fi
if [[ -z $(grep "net.ipv6.conf.default.disable_ipv6 = 1" "/etc/sysctl.conf") ]]; then echo "net.ipv6.conf.default.disable_ipv6 = 1" >> /etc/sysctl.conf; fi

xhost +local:
