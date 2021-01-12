-- only_tables:users user_infos  user_company  company_info company_subject

#删除触发器:
DROP TRIGGER IF EXISTS `users_ll_insert`; 
DROP TRIGGER IF EXISTS `users_ll_update`; 
DROP TRIGGER IF EXISTS `users_bl_insert`; 
DROP TRIGGER IF EXISTS `users_bl_update`; 
DROP TRIGGER IF EXISTS `user_infos_ll_insert`; 
DROP TRIGGER IF EXISTS `user_infos_ll_update`; 
DROP TRIGGER IF EXISTS `user_infos_bl_insert`; 
DROP TRIGGER IF EXISTS `user_infos_bl_update`; 
DROP TRIGGER IF EXISTS `company_subject_ll_insert`; 
DROP TRIGGER IF EXISTS `company_subject_ll_update`; 
DROP TRIGGER IF EXISTS `company_subject_bl_insert`; 
DROP TRIGGER IF EXISTS `company_subject_bl_update`;  
DROP TRIGGER IF EXISTS `company_info_bl_insert`; 
DROP TRIGGER IF EXISTS `company_info_bl_update`; 
DROP TRIGGER IF EXISTS `user_company_bl_insert`; 
DROP TRIGGER IF EXISTS `user_company_bl_update`; 