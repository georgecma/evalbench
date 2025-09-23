ALTER TABLE ONLY public.actuation_data
ADD CONSTRAINT actuation_data_pkey PRIMARY KEY (actreg);


ALTER TABLE ONLY public.maintenance_and_fault
ADD CONSTRAINT maintenance_and_fault_pkey PRIMARY KEY (upkeepactuation);


ALTER TABLE ONLY public.operation
ADD CONSTRAINT operation_pkey PRIMARY KEY (operreg);


ALTER TABLE ONLY public.performance_and_safety
ADD CONSTRAINT performance_and_safety_pkey PRIMARY KEY (effectivenessactuation);


ALTER TABLE ONLY public.robot_details
ADD CONSTRAINT robot_details_pkey PRIMARY KEY (botdetreg);


ALTER TABLE ONLY public.robot_record
ADD CONSTRAINT robot_record_pkey PRIMARY KEY (recreg);


ALTER TABLE ONLY public.system_controller
ADD CONSTRAINT system_controller_pkey PRIMARY KEY (systemoverseeractuation);


ALTER TABLE ONLY public.actuation_data
ADD CONSTRAINT actuation_data_actdetref_fkey FOREIGN KEY (actdetref) REFERENCES public.robot_details(botdetreg);


ALTER TABLE ONLY public.actuation_data
ADD CONSTRAINT actuation_data_actoperref_fkey FOREIGN KEY (actoperref) REFERENCES public.operation(operreg);


ALTER TABLE ONLY public.actuation_data
ADD CONSTRAINT actuation_data_actrecref_fkey FOREIGN KEY (actrecref) REFERENCES public.robot_record(recreg);


ALTER TABLE ONLY public.joint_condition
ADD CONSTRAINT joint_condition_jcdetref_fkey FOREIGN KEY (jcdetref) REFERENCES public.robot_details(botdetreg);


ALTER TABLE ONLY public.joint_condition
ADD CONSTRAINT joint_condition_jcondoperref_fkey FOREIGN KEY (jcondoperref) REFERENCES public.operation(operreg);


ALTER TABLE ONLY public.joint_condition
ADD CONSTRAINT joint_condition_jcrecref_fkey FOREIGN KEY (jcrecref) REFERENCES public.robot_record(recreg);


ALTER TABLE ONLY public.joint_performance
ADD CONSTRAINT joint_performance_jperfdetref_fkey FOREIGN KEY (jperfdetref) REFERENCES public.robot_details(botdetreg);


ALTER TABLE ONLY public.joint_performance
ADD CONSTRAINT joint_performance_jperfoperref_fkey FOREIGN KEY (jperfoperref) REFERENCES public.operation(operreg);


ALTER TABLE ONLY public.joint_performance
ADD CONSTRAINT joint_performance_jperfrecref_fkey FOREIGN KEY (jperfrecref) REFERENCES public.robot_record(recreg);


ALTER TABLE ONLY public.maintenance_and_fault
ADD CONSTRAINT maintenance_and_fault_upkeepactuation_fkey FOREIGN KEY (upkeepactuation) REFERENCES public.actuation_data(actreg);


ALTER TABLE ONLY public.maintenance_and_fault
ADD CONSTRAINT maintenance_and_fault_upkeepoperation_fkey FOREIGN KEY (upkeepoperation) REFERENCES public.operation(operreg);


ALTER TABLE ONLY public.maintenance_and_fault
ADD CONSTRAINT maintenance_and_fault_upkeeprobot_fkey FOREIGN KEY (upkeeprobot) REFERENCES public.robot_details(botdetreg);


ALTER TABLE ONLY public.mechanical_status
ADD CONSTRAINT mechanical_status_mechactref_fkey FOREIGN KEY (mechactref) REFERENCES public.actuation_data(actreg);


ALTER TABLE ONLY public.mechanical_status
ADD CONSTRAINT mechanical_status_mechdetref_fkey FOREIGN KEY (mechdetref) REFERENCES public.robot_details(botdetreg);


ALTER TABLE ONLY public.mechanical_status
ADD CONSTRAINT mechanical_status_mechoperref_fkey FOREIGN KEY (mechoperref) REFERENCES public.operation(operreg);


ALTER TABLE ONLY public.operation
ADD CONSTRAINT operation_operbotdetref_fkey FOREIGN KEY (operbotdetref) REFERENCES public.robot_details(botdetreg);


ALTER TABLE ONLY public.operation
ADD CONSTRAINT operation_operrecref_fkey FOREIGN KEY (operrecref) REFERENCES public.robot_record(recreg);


ALTER TABLE ONLY public.performance_and_safety
ADD CONSTRAINT performance_and_safety_effectivenessactuation_fkey FOREIGN KEY (effectivenessactuation) REFERENCES public.actuation_data(actreg);


ALTER TABLE ONLY public.performance_and_safety
ADD CONSTRAINT performance_and_safety_effectivenessoperation_fkey FOREIGN KEY (effectivenessoperation) REFERENCES public.operation(operreg);


ALTER TABLE ONLY public.performance_and_safety
ADD CONSTRAINT performance_and_safety_effectivenessrobot_fkey FOREIGN KEY (effectivenessrobot) REFERENCES public.robot_details(botdetreg);


ALTER TABLE ONLY public.robot_details
ADD CONSTRAINT robot_details_botdetreg_fkey FOREIGN KEY (botdetreg) REFERENCES public.robot_record(recreg);


ALTER TABLE ONLY public.system_controller
ADD CONSTRAINT system_controller_systemoverseeractuation_fkey FOREIGN KEY (systemoverseeractuation) REFERENCES public.actuation_data(actreg);


ALTER TABLE ONLY public.system_controller
ADD CONSTRAINT system_controller_systemoverseeroperation_fkey FOREIGN KEY (systemoverseeroperation) REFERENCES public.operation(operreg);


ALTER TABLE ONLY public.system_controller
ADD CONSTRAINT system_controller_systemoverseerrobot_fkey FOREIGN KEY (systemoverseerrobot) REFERENCES public.robot_details(botdetreg);


GRANT ALL ON SCHEMA public TO PUBLIC;

