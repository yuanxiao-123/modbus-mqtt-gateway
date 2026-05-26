#include <stdio.h>
#include <modbus.h>

int main(){
	modbus_t *ctx=modbus_new_rtu("/dev/pts/3",9600,'N',8,1);
	if(!ctx){
		perror("modbus_new_rtu");
		return 1;
	}
	modbus_set_slave(ctx,1);
	if(modbus_connect(ctx)<0){
		perror("modbus_connect");
		modbus_free(ctx);
		return 1;
	}
	modbus_mapping_t *mapping=modbus_mapping_new(0,0,100,0);
	if(!mapping){
		perror("modbus_mapping_new");
		return 1;
	}
	mapping->tab_registers[0]=256;
	mapping->tab_registers[1]=602;
	printf("slave start,waiting master ask\n");
	uint8_t query[MODBUS_RTU_MAX_ADU_LENGTH];
	while(1){
		int rc=modbus_receive(ctx,query);
		if(rc>0){
			modbus_reply(ctx,query,rc,mapping);
			printf("receive ask,actived,register0=%d register1=%d\n",
					mapping->tab_registers[0],
					mapping->tab_registers[1]);
		}
	}
	modbus_mapping_free(mapping);
	modbus_close(ctx);
	modbus_free(ctx);
	return 0;
}

