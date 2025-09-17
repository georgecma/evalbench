ALTER TABLE ONLY public.articles
ADD CONSTRAINT articles_pkey PRIMARY KEY (artkey);


ALTER TABLE ONLY public.devices
ADD CONSTRAINT devices_pkey PRIMARY KEY (devkey);


ALTER TABLE ONLY public.interactionmetrics
ADD CONSTRAINT interactionmetrics_pkey PRIMARY KEY (intmetkey);


ALTER TABLE ONLY public.interactions
ADD CONSTRAINT interactions_pkey PRIMARY KEY (intkey);


ALTER TABLE ONLY public.recommendations
ADD CONSTRAINT recommendations_pkey PRIMARY KEY (reckey);


ALTER TABLE ONLY public.sessions
ADD CONSTRAINT sessions_pkey PRIMARY KEY (seshkey);


ALTER TABLE ONLY public.systemperformance
ADD CONSTRAINT systemperformance_pkey PRIMARY KEY (rectrace);


ALTER TABLE ONLY public.users
ADD CONSTRAINT users_pkey PRIMARY KEY (userkey);


ALTER TABLE ONLY public.articles
ADD CONSTRAINT articles_authref_fkey FOREIGN KEY (authref) REFERENCES public.users(userkey);


ALTER TABLE ONLY public.devices
ADD CONSTRAINT devices_uselink_fkey FOREIGN KEY (uselink) REFERENCES public.users(userkey);


ALTER TABLE ONLY public.interactionmetrics
ADD CONSTRAINT interactionmetrics_intmetkey_fkey FOREIGN KEY (intmetkey) REFERENCES public.interactions(intkey);


ALTER TABLE ONLY public.interactions
ADD CONSTRAINT interactions_reclink_fkey FOREIGN KEY (reclink) REFERENCES public.recommendations(reckey);


ALTER TABLE ONLY public.interactions
ADD CONSTRAINT interactions_seshlink2_fkey FOREIGN KEY (seshlink2) REFERENCES public.sessions(seshkey);


ALTER TABLE ONLY public.recommendations
ADD CONSTRAINT recommendations_artlink_fkey FOREIGN KEY (artlink) REFERENCES public.articles(artkey);


ALTER TABLE ONLY public.sessions
ADD CONSTRAINT sessions_devrel_fkey FOREIGN KEY (devrel) REFERENCES public.devices(devkey);


ALTER TABLE ONLY public.sessions
ADD CONSTRAINT sessions_userel_fkey FOREIGN KEY (userel) REFERENCES public.users(userkey);


ALTER TABLE ONLY public.systemperformance
ADD CONSTRAINT systemperformance_devlink_fkey FOREIGN KEY (devlink) REFERENCES public.devices(devkey);


ALTER TABLE ONLY public.systemperformance
ADD CONSTRAINT systemperformance_seshlink_fkey FOREIGN KEY (seshlink) REFERENCES public.sessions(seshkey);


GRANT ALL ON SCHEMA public TO PUBLIC;

