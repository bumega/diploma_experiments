
from pathlib import Path
import pandas as pd
root = Path('/home/eliseev/nirs/results_multi_seed_B_real_tuned')
cls_files = list(root.rglob('all_classical_folds.csv'))
un_files = list(root.rglob('all_unary_folds.csv'))
print('CLS_FILES', len(cls_files))
print('UN_FILES', len(un_files))
if cls_files:
    dfc = pd.concat([pd.read_csv(p) for p in cls_files], ignore_index=True)
    # normalize method name if needed
    print('CLASSICAL_ROWS', len(dfc))
    g = dfc.groupby(['reducer','n_select']).agg(
        AUC_mean=('AUC','mean'),
        AUC_std=('AUC','std'),
        PR_mean=('PR_AUC','mean'),
        PR_std=('PR_AUC','std'),
        n=('AUC','size')
    ).reset_index().sort_values(['AUC_mean'], ascending=False)
    print('CLASSICAL_BY_METHOD_Q')
    print(g.to_csv(index=False))
    gm = dfc.groupby(['reducer','n_select','model']).agg(
        AUC_mean=('AUC','mean'),
        PR_mean=('PR_AUC','mean'),
        n=('AUC','size')
    ).reset_index()
    # Ensemble head-to-head ranks within q,model
    piv_auc = gm.pivot_table(index=['n_select','model'], columns='reducer', values='AUC_mean')
    piv_pr = gm.pivot_table(index=['n_select','model'], columns='reducer', values='PR_mean')
    for label,piv in [('AUC',piv_auc),('PR',piv_pr)]:
        counts={1:0,2:0,3:0,4:0}
        losses=[]
        if 'Ensembleunar' in piv.columns:
            for idx,row in piv.iterrows():
                s=row.dropna().sort_values(ascending=False)
                if 'Ensembleunar' not in s.index:
                    continue
                rank=list(s.index).index('Ensembleunar')+1
                counts[rank]=counts.get(rank,0)+1
                if rank!=1:
                    winner=s.index[0]
                    losses.append((idx[0],idx[1],winner,float(s.iloc[0]-s['Ensembleunar'])))
        print(f'RANKS_{label}', counts)
        print(f'TOP_LOSSES_{label}')
        for item in sorted(losses, key=lambda x:x[3], reverse=True)[:12]:
            print(item)
if un_files:
    dfu = pd.concat([pd.read_csv(p) for p in un_files], ignore_index=True)
    print('UNARY_ROWS', len(dfu))
    gu = dfu.groupby(['reducer','n_select']).agg(
        S_test_mean=('S_test','mean'),
        S_test_std=('S_test','std'),
        F12_mean=('F12','mean'),
        G12_mean=('G12','mean'),
        coverage_mean=('coverage','mean'),
        conflict_mean=('conflict_rate','mean'),
        n=('S_test','size')
    ).reset_index().sort_values(['S_test_mean'], ascending=False)
    print('UNARY_BY_METHOD_Q')
    print(gu.to_csv(index=False))
