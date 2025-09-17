ALTER TABLE ONLY public.additionalnotes
ADD CONSTRAINT additionalnotes_pkey PRIMARY KEY (notesreg);


ALTER TABLE ONLY public.commerceandcollection
ADD CONSTRAINT commerceandcollection_pkey PRIMARY KEY (commercereg);


ALTER TABLE ONLY public.engagement
ADD CONSTRAINT engagement_pkey PRIMARY KEY (engagereg);


ALTER TABLE ONLY public.eventsandclub
ADD CONSTRAINT eventsandclub_pkey PRIMARY KEY (eventsreg);


ALTER TABLE ONLY public.fans
ADD CONSTRAINT fans_pkey PRIMARY KEY (userregistry);


ALTER TABLE ONLY public.interactions
ADD CONSTRAINT interactions_pkey PRIMARY KEY (activityreg);


ALTER TABLE ONLY public.loyaltyandachievements
ADD CONSTRAINT loyaltyandachievements_pkey PRIMARY KEY (loyaltyreg);


ALTER TABLE ONLY public.membershipandspending
ADD CONSTRAINT membershipandspending_pkey PRIMARY KEY (memberreg);


ALTER TABLE ONLY public.moderationandcompliance
ADD CONSTRAINT moderationandcompliance_pkey PRIMARY KEY (modreg);


ALTER TABLE ONLY public.preferencesandsettings
ADD CONSTRAINT preferencesandsettings_pkey PRIMARY KEY (prefreg);


ALTER TABLE ONLY public.retentionandinfluence
ADD CONSTRAINT retentionandinfluence_pkey PRIMARY KEY (retreg);


ALTER TABLE ONLY public.socialcommunity
ADD CONSTRAINT socialcommunity_pkey PRIMARY KEY (socialreg);


ALTER TABLE ONLY public.supportandfeedback
ADD CONSTRAINT supportandfeedback_pkey PRIMARY KEY (supportreg);


ALTER TABLE ONLY public.virtualidols
ADD CONSTRAINT virtualidols_pkey PRIMARY KEY (entityreg);


ALTER TABLE ONLY public.commerceandcollection
ADD CONSTRAINT fk_commerce_engage FOREIGN KEY (commerceengagepivot) REFERENCES public.engagement(engagereg);


ALTER TABLE ONLY public.commerceandcollection
ADD CONSTRAINT fk_commerce_member FOREIGN KEY (commercememberpivot) REFERENCES public.membershipandspending(memberreg);


ALTER TABLE ONLY public.engagement
ADD CONSTRAINT fk_engage_activity FOREIGN KEY (engageactivitypivot) REFERENCES public.interactions(activityreg);


ALTER TABLE ONLY public.engagement
ADD CONSTRAINT fk_engage_member FOREIGN KEY (engagememberpivot) REFERENCES public.membershipandspending(memberreg);


ALTER TABLE ONLY public.eventsandclub
ADD CONSTRAINT fk_events_member FOREIGN KEY (eventsmemberpivot) REFERENCES public.membershipandspending(memberreg);


ALTER TABLE ONLY public.eventsandclub
ADD CONSTRAINT fk_events_social FOREIGN KEY (eventssocialpivot) REFERENCES public.socialcommunity(socialreg);


ALTER TABLE ONLY public.interactions
ADD CONSTRAINT fk_interact_fan FOREIGN KEY (interactfanpivot) REFERENCES public.fans(userregistry);


ALTER TABLE ONLY public.interactions
ADD CONSTRAINT fk_interact_idol FOREIGN KEY (interactidolpivot) REFERENCES public.virtualidols(entityreg);


ALTER TABLE ONLY public.loyaltyandachievements
ADD CONSTRAINT fk_loyalty_engage FOREIGN KEY (loyaltyengagepivot) REFERENCES public.engagement(engagereg);


ALTER TABLE ONLY public.loyaltyandachievements
ADD CONSTRAINT fk_loyalty_events FOREIGN KEY (loyaltyeventspivot) REFERENCES public.eventsandclub(eventsreg);


ALTER TABLE ONLY public.membershipandspending
ADD CONSTRAINT fk_member_fan FOREIGN KEY (memberfanpivot) REFERENCES public.fans(userregistry);


ALTER TABLE ONLY public.moderationandcompliance
ADD CONSTRAINT fk_mod_interact FOREIGN KEY (moderationinteractpivot) REFERENCES public.interactions(activityreg);


ALTER TABLE ONLY public.moderationandcompliance
ADD CONSTRAINT fk_mod_social FOREIGN KEY (moderationsocialpivot) REFERENCES public.socialcommunity(socialreg);


ALTER TABLE ONLY public.additionalnotes
ADD CONSTRAINT fk_notes_retain FOREIGN KEY (notesretainpivot) REFERENCES public.retentionandinfluence(retreg);


ALTER TABLE ONLY public.preferencesandsettings
ADD CONSTRAINT fk_pref_member FOREIGN KEY (preferencesmemberpivot) REFERENCES public.membershipandspending(memberreg);


ALTER TABLE ONLY public.preferencesandsettings
ADD CONSTRAINT fk_pref_social FOREIGN KEY (preferencessocialpivot) REFERENCES public.socialcommunity(socialreg);


ALTER TABLE ONLY public.retentionandinfluence
ADD CONSTRAINT fk_retain_engage FOREIGN KEY (retainengagepivot) REFERENCES public.engagement(engagereg);


ALTER TABLE ONLY public.retentionandinfluence
ADD CONSTRAINT fk_retain_loyalty FOREIGN KEY (retainloyaltypivot) REFERENCES public.loyaltyandachievements(loyaltyreg);


ALTER TABLE ONLY public.socialcommunity
ADD CONSTRAINT fk_social_commerce FOREIGN KEY (socialcommercepivot) REFERENCES public.commerceandcollection(commercereg);


ALTER TABLE ONLY public.socialcommunity
ADD CONSTRAINT fk_social_engage FOREIGN KEY (socialengagepivot) REFERENCES public.engagement(engagereg);


ALTER TABLE ONLY public.supportandfeedback
ADD CONSTRAINT fk_support_interact FOREIGN KEY (supportinteractpivot) REFERENCES public.interactions(activityreg);


ALTER TABLE ONLY public.supportandfeedback
ADD CONSTRAINT fk_support_pref FOREIGN KEY (supportprefpivot) REFERENCES public.preferencesandsettings(prefreg);


GRANT ALL ON SCHEMA public TO PUBLIC;

